bazel clean
bazel build //:Query --javacopt="-XepDisableAllChecks"
bazel build //:BatchInsert --javacopt="-XepDisableAllChecks"
bazel build //:Delete --javacopt="-XepDisableAllChecks"
bazel build //:Index --javacopt="-XepDisableAllChecks"