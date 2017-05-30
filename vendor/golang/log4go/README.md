log4go
======
filelog.go中，增加了按照日期分割文件的功能，但必须将rotate设为false，daily设为true才会生效。如果rotate设为true，日志切割走的就是原本的逻辑。
