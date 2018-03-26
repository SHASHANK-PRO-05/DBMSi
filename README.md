# DBMSi
This is a DBMSi project

This is a maven based project so you can navigate according to maven structure
	- If you want to check the code, it will be available in src directory
	- If you want to run the commands go to the bazel-bin directory and you can run the following commands there
		./BatchInsert sample.txt Minibase.min Employee1 4
		./Index Minibase.min Employee1 C BITMAP
		./Query Minibase.min Employee1 [ A C D B A ] C == 3 400 FILESCAN
		./Delete Minibase.min Employee1 C <= 3  4000 BTREE TRUE
	- Make sure your sample data is in that directory only.
	- There is also a typscript run available in the directory if you want a glance at the usage.
	
