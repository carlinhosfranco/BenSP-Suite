#       Benchmark for Stream Parallelism - BenSP

BenSP os a suite of parameterizable benchmarks for stream parallelism which is used to evaluate stream processing characteristics.

## How to cite BenSP

C. A. F. Maron, A. Vogel, D. Griebler and L. G. Fernandes. **Should PARSEC Benchmarks be More Parametric? A Case Study with Dedup.** 2019 27th Euromicro International Conference on Parallel, Distributed and Network-Based Processing (PDP), Pavia, Italy, 2019, pp. 217-221. ([PDF](https://doi.org/10.1109/EMPDP.2019.8671592))

## Folders Descripition

#### - LICENCE
Contains the licenses of PARSEC, GPLv3 and images licence used in the input sets.
#### - apps
Dedup and Ferret applications (for now).
#### - bin
Tool of the BenSP that manages the applications. The `parsec_stream` is the tool to execute, compile, and parameterize the Dedup and Ferret applications.
#### - logs
Folder to save all logs from BenSP.
#### - testbed
Set of scripts to automatize experiments.
#### - tools
Other tools created for BenSP.


<!--- ## Requirements [Sorry...](https://media.giphy.com/media/52qtwCtj9OLTi/giphy.gif)--->

## How to use BenSP
First, you must install all applications dependencies.

`command line to install`

BenSP Suite has several tools, librearies and files in many directories. To let your BenSP experience easier, the `parsec_stream` tool was built to parameterize, compile and run the BenSP benchmarks. Moreover, the tool organizes all logs of compilation and execution. To use `parsec_stream`, you must load the environment variables executing the command line bellow:

`command line to load`

BenSP Suite offers an input set to use. You can download the inputs set executing the script `download_input_sets.sh`. (The script will require your contact information and how/here the inputs will be used.) However, the benchmarks support your own input sets. If you wish to create another input, you need attention in some details [here](#how-to-create-your-own-input-set-for-dedup-and-ferret).

The `parsec_stream` tool makes easier your use of BenSP to evaluate the characteristics of Stream Processing. If it's your first use of parameterization benchmarks for this Stream Processing Domain, it's recommended that you understand the main parameterizable characteristics available in BenSP benchmarks. You can get this knowledge in [here] and read our article too.

The main arguments in `parsec_stream` are:

```
        -p PROGRAM       Program that will perform.
        -i INPUT         Input set to run the benchmarks. Default: '$default_inputsize'.
        -r REPLICAS      Number of replicas. Default: '$default_nreplicas'.
        -a ACTION        What do you do? run or change. See below for a list of valid changes.
        -h               Displays the help message.
```
If your choice was `run` option or you didn't do before any execution with change, the `parsec_stream` will be use as default the values found in original PARSEC version.


## How to create your own input set for Dedup and Ferret

Dedup and Ferret by BenSP Suite were modified from the original PARSEC version. To create your own input set, there are some details to follow. All limitations can be improved.

* Dedup  
The Dedup application supports more then one file, unlike the original PARSE version. However, these files must be in TAR format. Inside these files you can put any type of file.
This is a technical limitation of Dedup. The function that reads the file only processes TAR files.

* Ferret
The Ferret application supports only JPEG/JPG images. Although the application was made to process image, audio, video and 3D shapes, the PARSEC version of Ferret only processes images.
