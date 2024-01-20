# Dacompta

Show nice plots of data extracted from account statement files.

This may or may not be the ~third~ fourth time that I start over the project.

## Rules CSV
Situtated in the `rules` folder, looking like :
```
value;pattern;regex
AMAZON;AMAZON ;
Damien Lesecq;(M. )?DAMIEN LESECQ;1
O'TACOS;(?i)O ?tacos;1
SNCF;^(GARE )?SNCF;1
```
with :
* `value`: the value to replace with;
* `pattern`: string or regex;
* `regex`: indicates if `pattern` is a string or a regex.

If `pattern` is a string, dacompta will test the field to check if it begins with it, if it is a regex, dacompta will search for a match. 