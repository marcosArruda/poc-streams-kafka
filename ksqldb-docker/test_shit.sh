#!/usr/bin/env zsh
lines=$(cat porra)
while read line
do
    if [[ "$line" == "linha 2" ]]
    then
        foo=2
        echo "Variable \$foo updated to $foo inside if inside while loop"
    fi
    echo "Value of \$foo in while loop body: $foo"
done <<< "$(echo -e "$lines")"
