FILES="../../dataset.csv"
for f in $FILES;
do
  echo "Pocessing $f file ...";
  #set MONGODB_HOME environment variable  to point to the MongoDB installatio folder.
  ls -l $f;
  mongoimport --db projectdb --collection dataset --type csv --headerline $f;
done
