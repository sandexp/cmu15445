### GDB使用简易教程

CMU关于GDB使用教程: https://www.cs.cmu.edu/~gilpin/tutorial/

TechBeamers关于GDB教程: https://www.techbeamers.com/how-to-use-gdb-top-debugging-tips/

#### GDB常用指令

|指令名称|指令执行内容|
|---|---|
|backtrace|查看调用栈信息|
|info|查看当前栈帧变量的值|
|next|单步到下一条指令|
|frame|选择栈帧|
|step|执行下一条命令,如果是函数则进入函数内部执行|
|list 行号|显示从指定行号开始的源代码|
|list 函数名称|列出某个函数的源代码|
|break 行号|对某一行设置断点|
|break 函数名称|设置断点在某个函数的开始处|
|continue|跳到下一个断点(不是单步执行)|
|delete breakpoints|删除断点|
|disable breakpoints|禁用断点|
|enable breakpoints|启用断点|
|info breakpoints|查看断点信息|
|run|从头开始连续执行|
|display 变量名|跟踪查看某一个变量,每次停止下来都会查看它的值|
|undisplay|取消跟踪变量的设置|
|watch|设置观察点|
|info watchpoints|设置观察点信息|
|x|从给定位置开始打印存储器的一部分内容|