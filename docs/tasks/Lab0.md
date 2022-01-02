#### PROJECT SPECIFICATION
In this project, you will implement three classes: Matrix, RowMatrix, and RowMatrixOperations. These matrices are simple two-dimensional matrices that must support addition, matrix multiplication, and a simplified General Matrix Multiply (GEMM) operation.

You will only need to modify a single file: p0_starter.h You can find the file in the BusTub repository at src/include/primer/p0_starter.h.

In this header file, we define the three classes that you must implement. The Matrix abstract class defines the common functions for the derived class RowMatrix. The RowMatrixOperations class uses RowMatrix objects to achieve the operations mentioned in the overview above. The function prototypes and member variables are specified in the file. The project requires you to fill in the implementations of all the constructors, destructors, and member functions. Do not add any additional function prototypes or member variables. Your implementation should consist solely of implementing the functions that we have defined for you.

The instructor and TAs will not provide any assistance on C++ questions. You can freely use Google or StackOverflow to learn about C++ and any errors you encounter.

#### SETTING UP YOUR DEVELOPMENT ENVIRONMENT

First install the packages that BusTub requires:
```bash
$ sudo ./build_support/packages.sh
```
See the README for additional information on how to setup different OS environments.

To build the system from the commandline, execute the following commands:
```bash
$ mkdir build
$ cd build
$ cmake ..
$ make
```

To speed up the build process, you can use multiple threads by passing the `-j` flag to `make`. 
For example, the following command will build the system using four threads:

```bash
$ make -j 4
```

#### TESTING

You can test the individual components of this assignment using our testing framework. We use GTest for unit test cases. 
There is one file that contain tests for all three classes:

`Starter`: `test/primer/starter_test.cpp`

You can compile and run each test individually from the command-line:
```bash
$ mkdir build
$ cd build
$ make starter_test
$ ./test/starter_test
```

You can also run make check-tests to run ALL of the test cases. Note that some tests are disabled as you have not implemented future projects. 
You can disable tests in GTest by adding a `DISABLED_` prefix to the test name.

These tests are only a subset of the all the tests that we will use to evaluate and grade your project. You should write additional test cases on your own to check the complete functionality of your implementation.