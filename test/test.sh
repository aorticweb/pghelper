docker run --name pghelper-test --publish 5432:5432 -e POSTGRES_PASSWORD=pass -e POSTGRES_USER=aorticweb -d postgres > /dev/null 2> /dev/null
export PGHOST=$(docker-machine ip default)
# export PGHOST=$(docker-machine ip default) for mac
# export PGHOST=localhost for linux 

export PGUSER=aorticweb
export PGPASSWORD=pass
export PGPORT=5432
psql -h $PGHOST -p $PGPORT -U $PGUSER -a -f ./test_table.sql > /dev/null 2> /dev/null
wait
pytest
wait
docker kill pghelper-test > /dev/null 2> /dev/null
docker rm pghelper-test > /dev/null 2> /dev/null
