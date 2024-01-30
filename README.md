# Dacompta

Show nice plots of data extracted from account statement files.

This may or may not be the ~third~ fourth time that I start over the project.

## Rules CSV
Situtated in the `rules` folder, looking like :
```
value;method;pattern;method
AMAZON;set;AMAZON ;startswith
Damien Lesecq;set;(M. )?DAMIEN LESECQ;regex
O'TACOS;set;(?i)O ?tacos;regex
SNCF;set;^(GARE )?SNCF;regex
```
with :
* `value`: the value used at the end;
* `method` (first one): `set`, `begin`, `end`
* `pattern`: string or regex;
* `method`: `equals`, `startswith`, `endswith` or `regex` (will perform a `re.search`).

New CSV files are allowed to use multiple criterias :
```value;method;pattern;method;pattern;method```