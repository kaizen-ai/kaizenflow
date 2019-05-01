perl -e 'print join("\n", grep { not $seen{$_}++ } split(/:/, scalar <>))' | sort | uniq
