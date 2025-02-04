This package is mostly lifted from TopoLVM's internal/lvmd/command. It can't be referenced by our code, so we had to copy it. Original source code is Apache License V2, https://github.com/topolvm/topolvm/blob/main/LICENSE

Some changes have been applied to this package:

* controller-runtime logging is removed and replaced with klog/v2 as used elsewhere in the project.
* tests are removed since they referenced more internal packaged code that can't be used
* lvm_vgs.go includes new code to create VGs