# Pitchfork
Pitchfork is part of bfs, it's for probe Store and feed back to Directory

Table of Contents
=================

* [Features](#features)
* [Architechure](#architechure)
	* [Pitchfork](#pitchfork)
    * [Store](#store)
* [Installation](#installation)

## Features
* Mostly probe all store nodes and feed back to all directorys
* Adaptive Designs when store nodes change or pitchfork nodes change
* High-low coupling pitchfork feed back to directory through zookeeper

[Back to TOC](#table-of-contents)

## Architechure
### Pitchfork
Pitchfork contains unique id of pitchfork

### Store
Store contains unique id, rack position in zookeeper and accessed host

[Back to TOC](#table-of-contents)

## Installation

just pull `Terry-Mao/bfs` from github using `go get`:

```sh
$ go get github.com/Terry-Mao/bfs
$ cd $GOPATH/github.com/Terry-Mao/bfs/pitchfork
$ go build
```

[Back to TOC](#table-of-contents)

Have Fun!
