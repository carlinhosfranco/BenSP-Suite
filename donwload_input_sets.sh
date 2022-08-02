#!/bin/bash
#Scritp to install all input sets of BenSP suite.
#Author: Carlos Alberto Franco Maron
#Date: 03/23/2018

function download {
    $STATUS=""

    echo "wget LINK"
    STATUS=$(echo $?)

    if [ $STATUS !-eq 0  ]; then
        echo -e "Server not found. \n Please, check your connection internet."
    fi

}

function install {

    

}