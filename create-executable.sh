bazel clean
bazel build //:Query --javacopt="-XepDisableAllChecks"
bazel build //:BatchInsert --javacopt="-XepDisableAllChecks"
bazel build //:Delete --javacopt="-XepDisableAllChecks"
bazel build //:Index --javacopt="-XepDisableAllChecks"
bazel build //:TestDriver --javacopt="-XepDisableAllChecks"
bazel build //:ColumnSortScan --javacopt="-XepDisableAllChecks"
bazel build //:ColumnIndexScan --javacopt="-XepDisableAllChecks"