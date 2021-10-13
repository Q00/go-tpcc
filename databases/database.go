package databases

import (
	"context"
	"time"

	"github.com/Percona-Lab/go-tpcc/databases/elasticsearch"
	"github.com/Percona-Lab/go-tpcc/databases/mongodb"
	"github.com/Percona-Lab/go-tpcc/databases/mysql"
	"github.com/Percona-Lab/go-tpcc/databases/postgresql"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
)

type Database interface {
	StartTrx() error
	CommitTrx(ctx context.Context) error
	RollbackTrx(ctx context.Context) error
	CreateSchema() error
	CreateIndexes() error
	InsertOne(ctx context.Context, ableName string, d interface{}) error
	InsertBatch(ctx context.Context, tableName string, d []interface{}) error
	IncrementDistrictOrderId(ctx context.Context, warehouseId int, districtId int) error
	GetNewOrder(ctx context.Context, warehouseId int, districtId int) (*models.NewOrder, error)
	CheckNewOrder(ctx context.Context, warehouseId int, districtId int) (*models.NewOrder, *string, error)
	DeleteNewOrder(ctx context.Context, orderId int, warehouseId int, districtId int) error
	GetCustomer(ctx context.Context, customerId int, warehouseId int, districtId int) (*models.Customer, error)
	GetCustomerIdOrder(ctx context.Context, orderId int, warehouseId int, districtId int) (int, error)
	UpdateOrders(ctx context.Context, orderId int, warehouseId int, districtId int, oCarrierId int, deliveryDate time.Time) error
	SumOLAmount(ctx context.Context, orderId int, warehouseId int, districtId int) (float64, error)
	UpdateCustomer(ctx context.Context, customerId int, warehouseId int, districtId int, sumOlTotal float64) error
	GetNextOrderId(ctx context.Context, warehouseId int, districtId int) (int, error)
	GetStockCount(ctx context.Context, orderIdLt int, orderIdGt int, threshold int, warehouseId int, districtId int) (int64, error)
	GetCustomerById(ctx context.Context, customerId int, warehouseId int, districtId int) (*models.Customer, error)
	GetCustomerByName(ctx context.Context, name string, warehouseId int, districtId int) (*models.Customer, error)
	GetLastOrder(ctx context.Context, customerId int, warehouseId int, districtId int) (*models.Order, error)
	GetOrderLines(ctx context.Context, orderId int, warehouseId int, districtId int) (*[]models.OrderLine, error)
	GetWarehouse(ctx context.Context, warehouseId int) (*models.Warehouse, error)
	UpdateWarehouseBalance(ctx context.Context, warehouseId int, amount float64) error
	GetDistrict(ctx context.Context, warehouseId int, districtId int) (*models.District, error)
	UpdateDistrictBalance(ctx context.Context, warehouseId int, districtId int, amount float64) error
	InsertHistory(ctx context.Context, warehouseId int, districtId int, date time.Time, amount float64, data string) error
	UpdateCredit(ctx context.Context, customerId int, warehouseId int, districtId int, balance float64, data string) error
	CreateOrder(ctx context.Context, orderId int, customerId int, warehouseId int, districtId int, oCarrierId int, oOlCnt int, allLocal int, orderEntryDate time.Time, orderLine []models.OrderLine) error
	GetItems(ctx context.Context, itemIds []int) (*[]models.Item, error)
	UpdateStock(ctx context.Context, stockId int, warehouseId int, quantity int, ytd int, ordercnt int, remotecnt int) error
	GetStockInfo(ctx context.Context, districtId int, iIds []int, iWids []int, allLocal int) (*[]models.Stock, error)
}

func NewDatabase(driver, uri, dbname, username, password string, transactions bool, findandmodify bool) (Database, error) {
	var d Database
	var err error

	switch driver {
	case "mongodb":
		d, err = mongodb.NewMongoDb(uri, dbname, transactions, findandmodify)
	case "mysql":
		d, err = mysql.NewMySQL(uri, dbname, transactions)
	case "postgresql":
		d, err = postgresql.NewPostgreSQL(uri, dbname, transactions)
	case "elasticSearch":
		d, err = elasticsearch.NewElasticSearch(uri, findandmodify)
	default:
		panic("Unknown database driver")
	}

	if err != nil {
		return nil, err
	}

	return d, nil
}
