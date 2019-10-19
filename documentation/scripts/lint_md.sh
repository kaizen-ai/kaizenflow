#!/bin/bash -e

FILES=$(ls documentation/notes/*.md)

documentation/scripts/gh-md-toc --insert $FILES
