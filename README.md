#       Benchmark for Stream Parallelism - BenSP

BenSP is a suite of parameterizable benchmarks for stream parallelism which is used to evaluate stream processing characteristics.

Please, feel free to contact me: francocaam at gmail dot com

## How to cite BenSP

C. A. F. Maron, A. Vogel, D. Griebler and L. G. Fernandes. **Should PARSEC Benchmarks be More Parametric? A Case Study with Dedup.** 2019 27th Euromicro International Conference on Parallel, Distributed and Network-Based Processing (PDP), Pavia, Italy, 2019, pp. 217-221. ([PDF](https://doi.org/10.1109/EMPDP.2019.8671592))

## Folders Descripition

#### - LICENCE
Contains the licenses of PARSEC, GPLv3 and from the images used in the input sets.
#### - apps
Dedup and Ferret applications (for now).
#### - bin
BenSP's utilies to manage the applications. The `parsec_stream` is the utility to compile, parameterize and execute the Dedup and Ferret applications.
#### - logs
Folder to save all logs from BenSP.
#### - testbed
Set of scripts to automate experiments.
#### - tools
Other tools created for BenSP.

## Dependences 

[Utility Performance Library (UPL)](https://github.com/dalvangriebler/upl).

## How to use BenSP
Start by installing all applications dependencies.

`sudo apt-get install make build-essential m4 x11proto-xext-dev libglu1-mesa-dev libxi-dev libxmu-dev libtbb-dev libbz2-dev zlib1g-dev libgsl-dev libjpeg-dev`

BenSP Suite has several tools, librearies and files in many directories. In order to improve your experience with BenSP, the `parsec_stream` tool was built to parameterize, compile and run the BenSP benchmarks. Moreover, the tool organizes all logs of compiling and executing. To use `parsec_stream`, you must load the environment variables by executing the command line bellow:

`source envorinment_var.sh`

BenSP Suite offers input sets to use. You can download the input sets by executing the script `download_input_sets.sh`. (The script will require your contact information and how/here the inputs will be used.) Moreover, the benchmarks support your own custom input sets. If you wish to create another input, consider the related details shown [here](#how-to-create-your-own-custom-input-set-for-dedup-and-ferret).

The `parsec_stream` tool makes easier your use of BenSP to evaluate the characteristics of Stream Processing. If it's your first time using of parametric benchmarks for this Stream Processing Domain, it's recommended that you understand the main parameterizable characteristics available in BenSP benchmarks. Basic information is presented [here] as well as on [GMAP](https://gmap.pucrs.br/gmap/home/index/eng) publications.

The main arguments in `parsec_stream` are:

```
        -p PROGRAM       binary that will be run
        -i INPUT         Input set used by the benchmark. Default: '$default_inputsize'
        -n REPLICAS      Number of parallel replicas. Default: '$default_nreplicas'
        -a ACTION        What do you want to do? run or change. See below for a list of valid changes.
        -h               Displays the help message.
```
If your choice was the `run` option or you didn't do any execution with change before, the `parsec_stream` will use as default the values from original PARSEC version.

### Examples of usage

`parsec_stream -p dedup -r 12 -i h1 -a chunk 2048 fr 40 dd 40 comp 40 rr 40 notrace`

`parsec_stream -p dedup -r 12 -i hs -a chunk 1024 dd 40 comp 40 fr 40 rr 40 trace`

`parsec_stream -p ferret -r 2 -i l1 -a precision 128`

`parsec_stream -p ferret -t 12 -i h2 -a precision 128 seg 5 ext 5 idx 5 rank 5 ranking 50 notrace`


## How to create your own custom input set for Dedup and Ferret

Dedup and Ferret by BenSP Suite were modified from the original PARSEC version. To create your own input set, there are some details to follow. All limitations can be improved.

* Dedup  
The Dedup application supports more then one file, unlike the original PARSEC version. However, these files must be in TAR format. Inside these files you can put any file type or format.
This is a technical limitation of Dedup. The function that reads the files only processes TAR files.

* Ferret
The Ferret application supports only JPEG/JPG images. Although the application was made to process image, audio, video and 3D shapes, the PARSEC version of Ferret only accepts images as input.
