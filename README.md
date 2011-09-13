---
Ocean Observatories Initiative Cyberinfrastructure  
Integrated Observatory Network (ION)  
ion-netcdf-iosp  

---

# Description
This codebase provides an I/O Service Provider (IOSP) for the NetCDF Java library that allows direct communication with the ION system, facilitating the retrieval of scientific data.

##NOTE:
For this IOSP to function properly at this time, it must be used with the OOICI version of the NetCDF Java library.  This can be found on the package server at http://ooici.net/releases or by obtaining and building the THREDDS project here: git://github.com:ooici-eoi/THREDDS.git

#Source

Obtain the ion-netcdf-iosp project by running:  

    git clone git@github.com:ooici-eoi/ion-netcdf-iosp.git


###Build the library
from the 'ion-netcdf-iosp' directory:  

    ant dist

This will resolve project dependencies, compile, and generate a jar file in 'dist/lib'