# Directory
Directory is part of bfs, it provides http apis for client

Table of Contents
=================

* [Features](#features)
* [Architechure](#architechure)
	* [Directory](#directory)
    * [Dispatcher](#dispatcher)
* [API](#api)
	* [Get](#get)
	* [Upload](#upload)
	* [Del](#del)
* [Installation](#installation)

## Features
* Scheduling module of bfs, directory provieds http api for client
* High availability and easy extension

[Back to TOC](#table-of-contents)

## API

### Get 

get a file

**URL**

http://DOMAIN/get

***HTTP Method***

GET

***Query String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| key       | true  | int64  | file key |
| cookie       | true  | int64  | file cookie |

e.g curl "http://localhost:6065/get?key=679114092262199341&cookie=2937"

***Get Response***

```json
{"vid":315,"stores":["192.168.0.1:6062","192.168.0.2:6062","192.168.0.3:6062"]}
```

### Upload

upload a file

**URL**

http://DOMAIN/upload

***HTTP Method***

POST multipart/form-data

***Form String***

| name     | required  | type | description |
| :-----     | :---  | :--- | :---      |
| num        | true  | int32  | num of files |

e.g curl -d "num=2" "http://localhost:6065/upload"

***Upload Response***

```json
{"keys":[679114092262199341,679114092740349989],"vid":315,"cookie":2937,"stores":["192.168.0.1:6062","192.168.0.2:6062","192.168.0.3:6062"]}
```

### Delete

delete a file

**URL**

http://DOMAIN/del

***HTTP Method***

POST application/x-www-form-urlencoded

***Query String***

| name      | required  | type | description |
| :-----    | :---  | :--- | :---      |
| key       | true  | int64  | file key |
| cookie    | true  | int32  | cookie   |

e.g curl -d "key=5&cookie=5" "http://localhost:6065/del"

***Del Response***

```json
{"vid":315,"stores":["192.168.0.1:6062","192.168.0.2:6062","192.168.0.3:6062"]}
```

[Back to TOC](#table-of-contents)

## Architechure
### Directory
Directory pull store status from zookeeper and update into memory

### Dispatcher
Dispatcher schedule client requests, and guarantee load balancing

[Back to TOC](#table-of-contents)

## Installation

just pull `Terry-Mao/bfs` from github using `go get`:

```sh
$ go get github.com/Terry-Mao/bfs
$ cd $GOPATH/github.com/Terry-Mao/bfs
$ go build
```

[Back to TOC](#table-of-contents)

Have Fun!
