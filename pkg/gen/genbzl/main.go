// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Command genbzl is used to generate bazel files which then get imported by
// the gen package's BUILD.bazel to facilitate hoisting these generated files
// back into the source tree.
//
// It's all a bit meta. The flow is that we invoke this binary inside the
// bazelutil/bazel-generate.sh script which writes out some bzl files with
// lists of targets which are then depended on in gen.bzl
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"html/template"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/errors"
)

var (
	outDir = flag.String("out-dir", "", "directory in which to place the generated files")
)

func main() {
	flag.Parse()
	if err := generate(*outDir); err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate files: %v\n", err)
		exit.WithCode(exit.UnspecifiedError())
	}
}

const header = `# Generated by genbzl

`

var tmpl = template.Must(template.New("file").Parse(
	header + `{{ .Variable }} = [{{ range .Targets }}
  "{{ . }}",{{end}}
]
`))

type templateData struct {
	Variable string
	Targets  []string
}

var targets = []*target{
	{
		filename: "protobuf.bzl",
		query:    "kind(go_proto_library, //pkg/...)",
		variable: "PROTOBUF_SRCS",
	},
	{
		filename: "gomock.bzl",
		query:    `labels("out", kind("_gomock_prog_exec rule",  //pkg/...:*))`,
		variable: "GOMOCK_SRCS",
	},
	{
		filename: "stringer.bzl",
		query:    `labels("outs",  filter("-stringer$", kind("genrule rule",  //pkg/...:*)))`,
		variable: "STRINGER_SRCS",
	},
	{
		filename: "execgen.bzl",
		query: `
let genrules =
  kind("genrule rule",  //pkg/...:*)
in labels("outs",  attr("tools", "execgen", $genrules)
  + attr("exec_tools", "execgen", $genrules))`,
		variable: "EXECGEN_SRCS",
	},
	{
		filename: "optgen.bzl",
		query: `
let targets = attr("exec_tools", "(opt|lang)gen",  kind("genrule rule",  //pkg/...:*))
in let og = labels("outs",  $targets)
in $og - filter(".*:.*(-gen|gen-).*", $og)`,
		variable: "OPTGEN_SRCS",
	},
	{
		filename: "excluded.bzl",
		query: `
let all = kind("generated file", //...:*)
in ($all ^ //pkg/ui/...:*)
  + ($all ^ labels("out", kind("_gomock_prog_gen rule",  //pkg/...:*)))
  + filter(".*:.*(-gen|gen-).*", $all)
  + //pkg/testutils/lint/passes/errcheck:errcheck_excludes.txt
  + //build/bazelutil:test_stamping.txt
  + labels("outs", //docs/generated/sql/bnf:svg)`,
		variable: "EXCLUDED_SRCS",
	},
	{
		filename: "misc.bzl",
		query: `
kind("generated file", //pkg/...:*)
  - labels("srcs", //pkg/gen:explicitly_generated)
  - labels("srcs", //pkg/gen:excluded)
`,
		variable: "MISC_SRCS",
	},
	{
		filename: "docs.bzl",
		variable: "DOCS_SRCS",
		query: `
kind("generated file", //docs/...:*)
  - labels("outs", //docs/generated/sql/bnf:svg)`,
	},
}

type target struct {
	filename string
	variable string
	query    string
}

func execQuery(q string) (results []string, _ error) {
	cmd := exec.Command("bazel", "query", q)
	var stdout bytes.Buffer
	var stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, errors.Wrapf(err,
			"failed to run %s: (stderr)\n%s", cmd, &stderr)
	}
	for sc := bufio.NewScanner(&stdout); sc.Scan(); {
		results = append(results, sc.Text())
	}
	sort.Strings(results)
	return results, nil
}

func generate(outDir string) error {
	for _, t := range targets {
		if err := t.write(outDir, nil); err != nil {
			return err
		}
	}
	for _, t := range targets {
		out, err := execQuery(t.query)
		if err != nil {
			return err
		}
		if err := t.write(outDir, out); err != nil {
			return err
		}
	}
	return nil
}

func (t *target) write(outDir string, out []string) error {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, templateData{
		Variable: t.variable,
		Targets:  out,
	}); err != nil {
		return errors.Wrapf(err, "failed to execute template for %s", t.filename)
	}
	f, err := os.Create(filepath.Join(outDir, t.filename))
	if err != nil {
		return errors.Wrapf(err, "failed to open file for %s", t.filename)
	}
	if _, err := io.Copy(f, &buf); err != nil {
		return errors.Wrapf(err, "failed to write file for %s", t.filename)
	}
	if err := f.Close(); err != nil {
		return errors.Wrapf(err, "failed to write file for %s", t.filename)
	}
	return nil
}
