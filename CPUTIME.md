Modified runtime is submoduled under `go/`.

1. To build locally (assumes `$CRDB` points to cockroach checkout):

```sh
cd $CRDB/go/src && ./make.bash # builds the go binary and puts it under $CRDB/go/bin
```
2. Set `$GOROOT` to whatever `$CRDB/go/bin/go env GOROOT` says
3. (optional) Add `$CRDB/go/bin` to `PATH`; ensure `which go` points to this
   version. If using IDEs, point them to `$CRDB/go` instead.
4. `dev build short` etc. should build things using the modified runtime

TODO: Mirror pre-built modified runtime into CRDB managed GCP buckets to skip
steps 1-2.
