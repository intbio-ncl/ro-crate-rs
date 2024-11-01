Working in Alpha

# Overview 

ro-crate-rs is a rust library, python library and CLI tool for interfacing with [Reserach Object Crates](https://www.researchobject.org/ro-crate/). 
This implementation was specifically created to address the challenges of data 
handling on automated robotic systems within an automated synthetic biology lab,
however it's noted that this could have more general applicability to other environments.
It's aim is to provide a robust, portable and scalable solution to dealing with 
RO-Crates within the varying software environments of a synthetic biology lab stack.

It enforces minimal RO-Crate structure adherence through it's 
defined data types, but equally allows the crate to be as expansive as needed. 

## Why a rust library
Rust is an increasingly popular systems programming language that is useful for systems 
that require performance, robustness and the ability to run in a myriad of different
computing environments; be it Windows, Linux or MacOS, or x86, ARM or any other 
embedded/IOT system. It is a compiled, statically typed language with a strong 
emphasis on type safety and has started to form the performant core of data libraries, 
such as Polars and DeltaLake.

In this library, the rust type system and the serde library have been leveraged to 
provide a structured core for handling RO-Crates with robust data handling, built in 
constraints as per the RO-Crate specification (v1.1) and sufficient flexibilty to add any 
metadata required.

The aim is that for regular, automated and ***consistent*** robotic experiements, this 
library can be relied upon to create RO-Crates for years to come.


## Why a CLI tool 
The CLI tool can be used to immediately interface with RO-Crates within a somewhat 
constrained and structured environment, however an understanding of RO-Crate 
is a realistic requirement to conform with schema.org specifications. The CLI tool 
should not be relied upon as your primary interface to RO-crates, however it does 
allow the tool to be used on any system with some form of terminal, be it headless
or ssh. 


## Why a python library wrapper
The python library (rocraters) allows the primary rust library to be leveraged in python. 
It's designed to be maximally flexible with minimal onboarding, allowing you to 
incorprate it into scrpits/ data pipelines as easily as possible. Python is a 
ubquitous language among scientists, so this provides an accessible tool to use 
ro-crate-rs within already developed/new Python based environments.
This library relies on you to have an understanding of the structure of an RO-Crate, 
but focuses more on the fact that some metadata is better than no metadata.

This is not the go-to python libary for RO-Crate interfacing, 
please see [ro-crate-py](https://github.com/ResearchObject/ro-crate-py) for a 
full python implementation.

# Libraries

1. [ro-crate-rs core](src/README.md)
2. [rocraters python bindings](python/README.md)
3. [cli tool](cli/README.md)

# Compatability 

RO-Crate v1.1 only

