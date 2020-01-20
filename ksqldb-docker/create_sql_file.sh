#!/usr/bin/env zsh

_caput='INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('

#de 3903 até 7857  = diff 3954
function gen_decimal_1() {
  echo '37.'$(( 3903 + ($(od -An -N4 -tu4 /dev/urandom) % 3954) ))', '
}

function gen_decimal_2() {
  echo '-122.'$(( $(od -An -N4 -tu4 /dev/urandom) % 4011 ))');'
}

function gen_string() {
  echo \'$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 8 | head -n 1)\'', '
}


echo -n '{"ksql": "' > stuff.json
echo -n '' > stuff.sql
for (( i = 0; i < 1000; i++ )); do
  x=$(gen_string)
  y=$(gen_decimal_1)
  z=$(gen_decimal_2)
  echo -n " $_caput$x$y$z" >> stuff.json
done

echo -n '","streamsProperties": {}}' >> stuff.json


#INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('18f4ea86', 37.3903, -122.0643);


#'18f4ea86', 37.3903, -122.0643
