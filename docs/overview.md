# NutsDB 概览

[![GoDoc](https://godoc.org/github.com/nutsdb/nutsdb?status.svg)](https://godoc.org/github.com/nutsdb/nutsdb)  [![Go Report Card](https://goreportcard.com/badge/github.com/nutsdb/nutsdb)](https://goreportcard.com/report/github.com/nutsdb/nutsdb)  [![Go](https://github.com/nutsdb/nutsdb/workflows/Go/badge.svg?branch=master)](https://github.com/nutsdb/nutsdb/actions) [![codecov](https://codecov.io/gh/nutsdb/nutsdb/branch/master/graph/badge.svg?token=CupujOXpbe)](https://codecov.io/gh/nutsdb/nutsdb) [![License](http://img.shields.io/badge/license-Apache_2-blue.svg?style=flat-square)](https://raw.githubusercontent.com/nutsdb/nutsdb/master/LICENSE) [![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go#database)

NutsDB 是一个用纯 Go 编写的简单、快速、可嵌入且持久的键/值存储。

它支持完全可序列化的事务以及 List、Set、SortedSet 等多种数据结构。 所有操作都发生在 Tx 内部。 Tx 代表一个事务，可以是只读的，也可以是读写的。 只读事务可以读取给定存储桶和给定键的值或迭代一组键值对。 读写事务可以从数据库中读取、更新和删除键。

欢迎对NutsDB感兴趣的加群、一起开发，具体看这个issue：https://github.com/nutsdb/nutsdb/issues/116。

## 关注nutsdb公众号

<img src="https://user-images.githubusercontent.com/6065007/221391600-4f53e966-c376-426e-9dec-27364a0704d1.png"   height = "300" alt="nutsdb公众号"/>
