# jobber
Little job queue thing for golang

[![](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://godoc.org/github.com/dcarbone/jobber)

A very basic example is provided in [main](example/main.go)

# Overview

At a high level, you create a single [Boss](jobber.go#L121), add named [Worker](jobber.go#L24)s to the Boss, and add 
 [Job](jobber.go#L13)s to the Workers.
 
## Boss
The Boss is the central controller for any / all workers.  You can have as many workers as you want under a single boss.

## Worker
Worker is responsible for completing a specific type of work.  The general idea is that workers have a unique name 
within a given Boss and they only do one type of job.  You can, of course, do whatever you want.

The default Worker is called [PitDroid](jobber.go#L39) and can be used out of the box or as a prototype for your own
worker implementation.

## Job
A Job can be any unit of work you wish to executed by a Worker.  It's interface is designed to be minimal.  The output
of `Process()` is directly passed to the channel returned by `RespondTo()`, unless the Worker has been terminated in
which case it is recommended an standard error of some sort be passed in.
