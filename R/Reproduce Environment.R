## ----------------------------------------------------------------
## Define the coding parameters used in the environment.
##
##      Authors: Shelby Golden, MS from Yale's YSPH DSDE group
## Date Created: April 30th, 2025
## 
## Description: In order to create a reproducible environment for
##              all project collaborators, the versions and kinds
##              of packages used need to be read in. To do this
##              effectively, the originator of the codebase will
##              need to initialize renv() and store all these
##              details in the renv() lockfile.
## 

## ----------------------------------------------------------------
## SET UP THE ENVIRONMENT

# renv() is an R package that facilitates coding environment predictability.
# Additional details can be found here: https://rstudio.github.io/renv/index.html
# 
# Below are the steps needed to be taken to reproduce the environment.


# Install and read in the renv() package.
install.packages("renv")
library("renv")


# Initialize the environment. This will install all the packages that are
# recognized as being used in the coding environment. It will also define
# the version of those packages that were used, ensuring all collaborators
# are on the same page.
# 
# If it looks like packages are not being included that should be, load them
# in using library() and rerun the renv:init() command.
renv::init()


# If additional packages are added after initializing the environment, record
# them into the renv() lockfile using snapshot().
renv::snapshot()


# For new mirrors of the project, load the environmental settings used.
renv::restore()






