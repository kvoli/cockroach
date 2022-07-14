// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type stubTime struct {
	syncutil.RWMutex
	t time.Time
}

func (s *stubTime) setTime(t time.Time) {
	s.RWMutex.Lock()
	defer s.RWMutex.Unlock()
	s.t = t
}

func (s *stubTime) TimeNow() time.Time {
	s.RWMutex.RLock()
	defer s.RWMutex.RUnlock()
	return s.t
}

func installTelemetryLogFileSink(sc *log.TestLogScope, t *testing.T) func() {
	// Enable logging channels.
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	// Make a sink for just the session log.
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"telemetry": {
			Channels: logconfig.SelectChannels(channel.TELEMETRY),
		}}
	dir := sc.GetDirectory()
	if err := cfg.Validate(&dir); err != nil {
		t.Fatal(err)
	}
	cleanup, err := log.ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}

	return cleanup
}

// TestTelemetryLogging verifies that telemetry events are logged to the telemetry log
// and are sampled according to the configured sample rate.
func TestTelemetryLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := stubTime{}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TelemetryLoggingKnobs: &TelemetryLoggingTestingKnobs{
				getTimeNow: st.TimeNow,
			},
		},
	})

	defer s.Stopper().Stop(context.Background())

	var sessionID string
	var databaseName string
	var dbID uint32

	db := sqlutils.MakeSQLRunner(sqlDB)
	conn := db.DB.(*gosql.DB)

	db.QueryRow(t, `SHOW session_id`).Scan(&sessionID)
	db.QueryRow(t, `SHOW database`).Scan(&databaseName)
	dbID = sqlutils.QueryDatabaseID(t, conn, databaseName)
	db.Exec(t, `SET application_name = 'telemetry-logging-test'`)
	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)
	db.Exec(t, "CREATE TABLE t();")

	// Testing Cases:
	// - entries that are NOT sampled
	// 	- cases include:
	//		- statement type not DML
	// - entries that ARE sampled
	// 	- cases include:
	//		- statement type DML, enough time has elapsed

	testData := []struct {
		name                    string
		query                   string
		execTimestampsSeconds   []float64 // Execute the query with the following timestamps.
		expectedLogStatement    string
		stubMaxEventFrequency   int64
		expectedSkipped         []int // Expected skipped query count per expected log line.
		expectedUnredactedTags  []string
		expectedApplicationName string
	}{
		{
			// Test case with statement that is not of type DML.
			// Even though the queries are executed within the required
			// elapsed interval, we should still see that they were all
			// logged since  we log all statements that are not of type DML.
			"truncate-table-query",
			"TRUNCATE t;",
			[]float64{1, 1.1, 1.2, 2},
			`TRUNCATE TABLE`,
			1,
			[]int{0, 0, 0, 0},
			[]string{"client"},
			"telemetry-logging-test",
		},
		{
			// Test case with statement that is of type DML.
			// The first statement should be logged.
			"select-*-limit-1-query",
			"SELECT * FROM t LIMIT 1;",
			[]float64{3},
			`SELECT * FROM \"\".\"\".t LIMIT ‹1›`,
			1,
			[]int{0},
			[]string{"client"},
			"telemetry-logging-test",
		},
		{
			// Test case with statement that is of type DML.
			// Two timestamps are within the required elapsed interval,
			// thus 2 log statements are expected, with 2 skipped queries.
			"select-*-limit-2-query",
			"SELECT * FROM t LIMIT 2;",
			[]float64{4, 4.1, 4.2, 5},
			`SELECT * FROM \"\".\"\".t LIMIT ‹2›`,
			1,
			[]int{0, 2},
			[]string{"client"},
			"telemetry-logging-test",
		},
		{
			// Test case with statement that is of type DML.
			// Once required time has elapsed, the next statement should be logged.
			"select-*-limit-3-query",
			"SELECT * FROM t LIMIT 3;",
			[]float64{6, 6.01, 6.05, 6.06, 6.1, 6.2},
			`SELECT * FROM \"\".\"\".t LIMIT ‹3›`,
			10,
			[]int{0, 3, 0},
			[]string{"client"},
			"telemetry-logging-test",
		},
	}

	for _, tc := range testData {
		telemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, tc.stubMaxEventFrequency)
		for _, execTimestamp := range tc.execTimestampsSeconds {
			stubTime := timeutil.FromUnixMicros(int64(execTimestamp * 1e6))
			st.setTime(stubTime)
			db.Exec(t, tc.query)
		}
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"sampled_query"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	for _, e := range entries {
		if strings.Contains(e.Message, `"ExecMode":"`+executorTypeInternal.logLabel()) {
			t.Errorf("unexpected telemetry event for internal statement:\n%s", e.Message)
		}
	}

	for _, tc := range testData {
		logCount := 0
		expectedLogCount := len(tc.expectedSkipped)
		// NB: FetchEntriesFromFiles delivers entries in reverse order.
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if strings.Contains(e.Message, tc.expectedLogStatement) {
				if logCount == expectedLogCount {
					t.Errorf("%s: found more than %d expected log entries", tc.name, expectedLogCount)
					break
				}
				expectedSkipped := tc.expectedSkipped[logCount]
				logCount++
				if expectedSkipped == 0 {
					if strings.Contains(e.Message, "SkippedQueries") {
						t.Errorf("%s: expected no skipped queries, found:\n%s", tc.name, e.Message)
					}
				} else {
					if expected := fmt.Sprintf(`"SkippedQueries":%d`, expectedSkipped); !strings.Contains(e.Message, expected) {
						t.Errorf("%s: expected %s found:\n%s", tc.name, expected, e.Message)
					}
				}
				costRe := regexp.MustCompile("\"CostEstimate\":[0-9]*\\.[0-9]*")
				if !costRe.MatchString(e.Message) {
					t.Errorf("expected to find CostEstimate but none was found")
				}
				distRe := regexp.MustCompile("\"Distribution\":(\"full\"|\"local\")")
				if !distRe.MatchString(e.Message) {
					t.Errorf("expected to find Distribution but none was found")
				}
				// Match StatementID on any non-empty string value.
				stmtID := regexp.MustCompile("\"StatementID\":(\"\\S+\")")
				if !stmtID.MatchString(e.Message) {
					t.Errorf("expected to find StatementID but none was found in: %s", e.Message)
				}
				// Match TransactionID on any non-empty string value.
				txnID := regexp.MustCompile("\"TransactionID\":(\"\\S+\")")
				if !txnID.MatchString(e.Message) {
					t.Errorf("expected to find TransactionID but none was found in: %s", e.Message)
				}
				for _, eTag := range tc.expectedUnredactedTags {
					for _, tag := range strings.Split(e.Tags, ",") {
						kv := strings.Split(tag, "=")
						if kv[0] == eTag && strings.ContainsAny(kv[0], fmt.Sprintf("%s%s", redact.StartMarker(), redact.EndMarker())) {
							t.Errorf("expected tag %s to be redacted within tags: %s", tag, e.Tags)
						}
					}
				}
				if !strings.Contains(e.Message, "\"ApplicationName\":\""+tc.expectedApplicationName+"\"") {
					t.Errorf("expected to find unredacted Application Name: %s", tc.expectedApplicationName)
				}
				if !strings.Contains(e.Message, "\"SessionID\":\""+sessionID+"\"") {
					t.Errorf("expected to find sessionID: %s", sessionID)
				}
				if !strings.Contains(e.Message, "\"Database\":\""+databaseName+"\"") {
					t.Errorf("expected to find Database: %s", databaseName)
				}
				if !strings.Contains(e.Message, "\"DatabaseID\":"+strconv.Itoa(int(dbID))) {
					t.Errorf("expected to find DatabaseID: %v", dbID)
				}
			}
		}
		if logCount != expectedLogCount {
			t.Errorf("%s: expected %d log entries, found %d", tc.name, expectedLogCount, logCount)
		}
	}
}

func TestNoTelemetryLogOnTroubleshootMode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := installTelemetryLogFileSink(sc, t)
	defer cleanup()

	st := stubTime{}

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			TelemetryLoggingKnobs: &TelemetryLoggingTestingKnobs{
				getTimeNow: st.TimeNow,
			},
		},
	})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(context.Background())

	db.Exec(t, `SET CLUSTER SETTING sql.telemetry.query_sampling.enabled = true;`)
	db.Exec(t, "CREATE TABLE t();")

	stubMaxEventFrequency := int64(1)
	telemetryMaxEventFrequency.Override(context.Background(), &s.ClusterSettings().SV, stubMaxEventFrequency)

	/*
		Testing Cases:
			- run query when troubleshoot mode is enabled
				- ensure no log appears
			- run another query when troubleshoot mode is disabled
				- ensure log appears
	*/
	testData := []struct {
		name                      string
		query                     string
		expectedLogStatement      string
		enableTroubleshootingMode bool
		expectedNumLogs           int
	}{
		{
			"select-troubleshooting-enabled",
			"SELECT * FROM t LIMIT 1;",
			`SELECT * FROM \"\".\"\".t LIMIT ‹1›`,
			true,
			0,
		},
		{
			"select-troubleshooting-disabled",
			"SELECT * FROM t LIMIT 2;",
			`SELECT * FROM \"\".\"\".t LIMIT ‹2›`,
			false,
			1,
		},
	}

	for idx, tc := range testData {
		// Set the time for when we issue a query to enable/disable
		// troubleshooting mode.
		setTroubleshootModeTime := timeutil.FromUnixMicros(int64(idx * 1e6))
		st.setTime(setTroubleshootModeTime)
		if tc.enableTroubleshootingMode {
			db.Exec(t, `SET troubleshooting_mode = true;`)
		} else {
			db.Exec(t, `SET troubleshooting_mode = false;`)
		}
		// Advance time 1 second from previous query. Ensure enough time has passed
		// from when we set troubleshooting mode for this query to be sampled.
		setQueryTime := timeutil.FromUnixMicros(int64((idx + 1) * 1e6))
		st.setTime(setQueryTime)
		db.Exec(t, tc.query)
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"sampled_query"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	for _, tc := range testData {
		numLogsFound := 0
		for i := len(entries) - 1; i >= 0; i-- {
			e := entries[i]
			if strings.Contains(e.Message, tc.expectedLogStatement) {
				if tc.enableTroubleshootingMode {
					t.Errorf("%s: unexpected log entry when troubleshooting mode enabled:\n%s", tc.name, entries[0].Message)
				} else {
					numLogsFound++
				}
			}
		}
		if numLogsFound != tc.expectedNumLogs {
			t.Errorf("%s: expected %d log entries, found %d", tc.name, tc.expectedNumLogs, numLogsFound)
		}
	}
}
