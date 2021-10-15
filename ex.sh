docker-compose -f e.yml down
docker volume rm go-tpcc_data01
docker volume rm go-tpcc_data02
docker volume rm go-tpcc_data03
docker-compose -f e.yml up -d
sleep 30s

./go-tpcc prepare  --threads 2 --warehouses 10 --uri 'http://127.0.0.1:9200' --db logs_db --dbdriver elasticsearch > execution2-10.txt
./go-tpcc run  --threads 2 --warehouses 10 --uri http://localhost:9200 --db logs_db --dbdriver elasticsearch --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > 2-10b.csv


docker-compose -f e.yml down
docker volume rm go-tpcc_data01
docker volume rm go-tpcc_data02
docker volume rm go-tpcc_data03
docker-compose -f e.yml up -d
sleep 10s
./go-tpcc prepare  --threads 10 --warehouses 20 --uri 'http://127.0.0.1:9200' --db logs_db --dbdriver elasticsearch  > execution10-20.txt

./go-tpcc run  --threads 10 --warehouses 20 --uri http://localhost:9200 --db logs_db --dbdriver elasticsearch --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > 10-20b.csv


docker-compose -f e.yml down
docker volume rm go-tpcc_data01
docker volume rm go-tpcc_data02
docker volume rm go-tpcc_data03
docker-compose -f e.yml up -d
sleep 10s
./go-tpcc prepare  --threads 10 --warehouses 30 --uri 'http://127.0.0.1:9200' --db logs_db --dbdriver elasticsearch  > execution10-30.txt
./go-tpcc run  --threads 10 --warehouses 30 --uri http://localhost:9200 --db logs_db --dbdriver elasticsearch --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > 10-30b.csv


docker-compose -f e.yml down
docker volume rm go-tpcc_data01
docker volume rm go-tpcc_data02
docker volume rm go-tpcc_data03

docker-compose -f m.yml up -d
sleep 10s


./go-tpcc prepare  --threads 2 --warehouses 10 --uri 'mongodb://127.0.0.1:27017' --db logs_db --dbdriver mongodb  > executionMongo2-10.txt

./go-tpcc run  --threads 2 --warehouses 10 --uri mongodb://localhost:9200 --db logs_db --dbdriver mongodb --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > m2-10b.csv


docker-compose -f m.yml down
docker volume rm go-tpcc_m01

docker-compose -f m.yml up -d
sleep 10s


./go-tpcc prepare  --threads 5 --warehouses 10 --uri 'mongodb://127.0.0.1:27017' --db logs_db --dbdriver mongodb > executionMongo5-10.txt
./go-tpcc run  --threads 5 --warehouses 10 --uri mongodb://localhost:9200 --db logs_db --dbdriver mongodb --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > m5-10b.csv


docker-compose -f m.yml down
docker volume rm go-tpcc_m01

docker-compose -f m.yml up -d
sleep 10s


./go-tpcc prepare  --threads 10 --warehouses 10 --uri 'mongodb://127.0.0.1:27017' --db logs_db --dbdriver mongodb > executionMongo10-10.txt
./go-tpcc run  --threads 10 --warehouses 10 --uri mongodb://localhost:271017 --db logs_db --dbdriver mongodb --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > m10-10b.csv

docker-compose -f m.yml down
docker volume rm go-tpcc_m01

docker-compose -f m.yml up -d
sleep 10s

./go-tpcc prepare  --threads 10 --warehouses 20 --uri 'mongodb://127.0.0.1:27017' --db logs_db --dbdriver mongodb > executionMongo10-20.txt

./go-tpcc run  --threads 10 --warehouses 20 --uri mongodb://localhost:271017 --db logs_db --dbdriver mongodb --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > m10-20b.csv

docker-compose -f m.yml down
docker volume rm go-tpcc_m01

docker-compose -f m.yml up -d
sleep 10s

./go-tpcc prepare  --threads 10 --warehouses 30 --uri 'mongodb://127.0.0.1:27017' --db logs_db --dbdriver mongodb > executionMongo10-30.txt
./go-tpcc run  --threads 10 --warehouses 30 --uri mongodb://localhost:271017 --db logs_db --dbdriver mongodb --time 100 --trx true --report-format csv --percentile 95 --report-interval 1 --percent-fail 0 > m10-30b.csv

