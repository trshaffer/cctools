






















# maker_wq(1)

## NAME
**maker_wq** - Run the Maker genome annotation tool using Work Queue to harness heterogenous resources

## SYNOPSIS
****maker_wq [options] <maker_opts> <maker_bopts> <maker_exe> ****

## DESCRIPTION
**maker_wq** is a master script to run the Maker genome annotation tool using Work Queue to enable the user to harness the heterogenous power of multiple systems simultaneously. It accepts all of the Maker inputs. The primary difference is that the MPI code has been replaced with Work Queue components.

**maker_wq** expects a maker_wq_worker in the path, and can be used from any working directory. All required input files are specified in the standard Maker control files just as in the standard Maker distribution.

## OPTIONS

- **-port port** Specify the port on which to create the Work Queue
- **-fa fast_abort** Specify a fast abort multiplier
- **-N project** Specify a project name for utilizing shared workers


## EXIT STATUS
On success, returns zero.  On failure, returns non-zero.

## ENVIRONMENT VARIABLES

## EXAMPLES

To run maker_wq, specify the same arguments as standard Maker:
```
maker_wq maker_opts.ctl maker_bopts.ctl maker_exe.ctl > output
```
This will begin the Maker run. All that is needed now is to submit workers that can be accessed by our master.

## COPYRIGHT

The Cooperative Computing Tools are Copyright (C) 2005-2019 The University of Notre Dame.  This software is distributed under the GNU General Public License.  See the file COPYING for details.

## SEE ALSO

CCTools 8.0.0 DEVELOPMENT released on 