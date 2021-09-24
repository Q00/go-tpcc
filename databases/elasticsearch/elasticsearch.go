package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	types "github.com/Percona-Lab/go-tpcc/databases/elasticsearch/models"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ElasticSearch struct {
	Client *elasticsearch.Client
	ctx    context.Context
}

// connect elasticsearch
func NewElasticSearch(uri string) (*ElasticSearch, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			uri,
		},
	}
	es, err := elasticsearch.NewClient(cfg)

	if err != nil {
		return nil, err
	}

	_, errIn := es.Info()
	if errIn != nil {
		log.Fatalf("Error getting response: %s", errIn)
		return nil, errIn
	}

	return &ElasticSearch{
		Client: es,
		ctx:    context.Background(),
	}, nil
}

func (db *ElasticSearch) CreateSchema() error {
	return nil
}

// transaction 은 es에서는 version update 변수로 작동하므로 pass

func (db *ElasticSearch) StartTrx() error {
	sess := mongo.SessionFromContext(db.ctx)
	err := sess.StartTransaction()
	if err != nil {
		return err
	}

	return nil
}

func (db *ElasticSearch) CommitTrx() error {
	sess := mongo.SessionFromContext(db.ctx)
	err := sess.CommitTransaction(db.ctx)
	if err != nil {
		return err
	}

	return nil
}

func (db *ElasticSearch) RollbackTrx() error {
	sess := mongo.SessionFromContext(db.ctx)
	err := sess.AbortTransaction(db.ctx)
	if err != nil {
		return err
	}

	return nil
}

// there is no need about indexing on elasticsearch

func (db *ElasticSearch) CreateIndexes() error {
	return nil
}

/*
func (db *ElasticSearch) CreateIndexes() error {
	ascending := bsonx.Int32(1)
	descending := bsonx.Int32(-1)

	_, err := db.C.Collection("ITEM").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"W_ID", ascending},
				{"W_TAX", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("WAREHOUSE").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"I_W_ID", ascending},
				{"I_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("DISTRICT").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"D_W_ID", ascending},
				{"D_ID", ascending},
				{"D_NEXT_O_ID", ascending},
				{"D_TAX", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("CUSTOMER").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"C_W_ID", ascending},
				{"C_D_ID", ascending},
				{"C_ID", ascending},
			},
		},
		{
			Keys: bsonx.Doc{
				{"C_W_ID", ascending},
				{"C_D_ID", ascending},
				{"C_LAST", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("STOCK").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"S_W_ID", ascending},
				{"S_I_ID", ascending},
				{"S_QUANTITY", ascending},
			},
		},
		{
			Keys: bsonx.Doc{
				{"S_I_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("ORDERS").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"O_W_ID", ascending},
				{"O_D_ID", ascending},
				{"O_ID", ascending},
				{"O_C_ID", ascending},
			},
		},
		{
			Keys: bsonx.Doc{
				{"O_C_ID", ascending},
				{"O_D_ID", ascending},
				{"O_W_ID", ascending},
				{"O_ID", descending},
				{"O_CARRIER_ID", ascending},
				{"O_ENTRY_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("NEW_ORDER").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"NO_W_ID", ascending},
				{"NO_D_ID", ascending},
				{"NO_O_ID", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("ORDER_LINE").Indexes().CreateMany(context.Background(), []mongo.IndexModel{
		{
			Keys: bsonx.Doc{
				{"OL_O_ID", ascending},
				{"OL_D_ID", ascending},
				{"OL_W_ID", ascending},
				{"OL_NUMBER", ascending},
			},
		},
		{
			Keys: bsonx.Doc{
				{"OL_O_ID", ascending},
				{"OL_D_ID", ascending},
				{"OL_W_ID", ascending},
				{"OL_I_ID", descending},
				{"OL_AMOUNT", ascending},
			},
		},
	})

	if err != nil {
		return err
	}

	return nil
}
*/

func (db *ElasticSearch) InsertOne(tableName string, d interface{}) (err error) {
	// request indexing
	dataJSON, err := json.Marshal(d)
	js := string(dataJSON)

	req := esapi.IndexRequest{
		Index:   tableName,
		Body:    strings.NewReader(js),
		Refresh: "true",
	}

	res, err := req.Do(db.ctx, db.Client)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error indexing document", res.Status())
	} else {
		var r types.ResponseES
		if errJs := json.NewDecoder(res.Body).Decode(&r); errJs != nil {
			log.Printf("Error parsing the response body: %s", errJs)
		} else {
			log.Printf("[%s] version=%d ID=%s Index=%s", res.Status(), r.Version, r.ID, r.Index)
		}
	}

	return nil
}

func (db *ElasticSearch) InsertBatch(tableName string, d []interface{}) error {

	retryOnConflit := new(int)
	*retryOnConflit = 3

	// request indexing
	indexer, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  tableName,
		Client: db.Client,
	})

	for _, a := range d {
		dataJSON, err := json.Marshal(a)
		if err != nil {
			log.Fatalf("cannot encode bulk %s", err)
		}

		err = indexer.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				// Action field configures the operation to perform (index, create, delete, update)
				Action: "index",

				// DocumentID is the (optional) document ID

				// Body is an `io.Reader` with the payload
				Body: bytes.NewReader(dataJSON),

				RetryOnConflict: retryOnConflit,

				// OnSuccess is called for each successful operation
				OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
					// SOMETHING
				},

				// OnFailure is called for each failed operation
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("ERROR: %s", err)
					} else {
						log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			log.Fatalf("Unexpected error: %s", err)
		}
	}

	// Close the indexer
	//
	if err := indexer.Close(context.Background()); err != nil {
		log.Fatalf("Unexpected error: %s", err)
	}

	return nil
}

// Get District using warehouseId and districtId and return pointer to models.District or error instead.
func (db *ElasticSearch) IncrementDistrictOrderId(warehouseId int, districtId int) error {
	var doc bytes.Buffer
	documentUp := map[string]interface{}{
		"scripts": "ctx._source.D_NEXT_O_ID += 1",
	}

	if err := json.NewEncoder(&doc).Encode(documentUp); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	m := types.Must{}
	m[0] = map[string]interface{}{
		"D_ID": districtId,
	}

	m[1] = map[string]interface{}{
		"D_W_ID": warehouseId,
	}

	var q types.BoolMustQuery

	q.Query.Bool.Must = m

	//!TODO: check version

	indexes := [1]string{"DISTRICT"}

	refresh := new(bool)
	*refresh = true

	qString, err := json.Marshal(q)

	req := esapi.UpdateByQueryRequest{
		Index:   indexes[:],
		Body:    &doc,
		Refresh: refresh,
		Pretty:  true,
		Query:   string(qString),
	}

	res, err := req.Do(db.ctx, db.Client)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error delete document", res.Status())
	} else {
		var r map[string]interface{}
		if errJs := json.NewDecoder(res.Body).Decode(&r); errJs != nil {
			log.Printf("Error parsing the response body: %s", errJs)
		} else {
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}

	return nil
}

// It also deletes new order, as ElasticSearch can do that findAndModify is set to 0
func (db *ElasticSearch) GetNewOrder(warehouseId int, districtId int) (*models.NewOrder, error) {
	var NewOrder models.NewOrder
	var err error

	newOrderProjection := bson.D{
		{"_id", 0},
		{"NO_D_ID", 1},
		{"NO_W_ID", 1},
		{"NO_O_ID", 1},
	}

	filter := bson.D{
		{"NO_D_ID", districtId},
		{"NO_W_ID", warehouseId},
	}

	newOrderSort := bson.D{{"NO_O_ID", 1}}

	if db.findAndModify {
		err = db.C.Collection("NEW_ORDER").FindOneAndDelete(
			db.ctx,
			filter,
			options.FindOneAndDelete().SetSort(newOrderSort).SetProjection(newOrderProjection),
		).Decode(&NewOrder)

		if err != nil {
			return nil, err
		}
	} else {
		err = db.C.Collection("NEW_ORDER").FindOne(
			db.ctx,
			filter,
			options.FindOne().SetProjection(newOrderProjection).SetSort(newOrderSort),
		).Decode(&NewOrder)
	}

	return &NewOrder, nil
}

func (db *ElasticSearch) DeleteNewOrder(orderId int, warehouseId int, districtId int) error {
	var err error

	filter := bson.D{
		{"NO_O_ID", orderId},
		{"NO_D_ID", districtId},
		{"NO_W_ID", warehouseId},
	}

	if db.findAndModify {
		return nil
	}

	r, err := db.C.Collection("NEW_ORDER").DeleteOne(db.ctx, filter, nil)

	if err != nil {
		return err
	}

	if r.DeletedCount == 0 {
		return fmt.Errorf("no documents found")
	}

	return nil
}

func (db *ElasticSearch) GetCustomer(customerId int, warehouseId int, districtId int) (*models.Customer, error) {
	var err error

	var c models.Customer

	err = db.C.Collection("CUSTOMER").FindOne(db.ctx, bson.D{
		{"C_ID", customerId},
		{"C_D_ID", districtId},
		{"C_W_ID", warehouseId},
	}).Decode(&c)

	if err != nil {
		return nil, err
	}

	return &c, nil
}

// GetCId
func (db *ElasticSearch) GetCustomerIdOrder(orderId int, warehouseId int, districtId int) (int, error) {
	var err error

	filter := bson.D{
		{"O_ID", orderId},
		{"O_D_ID", districtId},
		{"O_W_ID", warehouseId},
	}

	var doc bson.M
	err = db.C.Collection("ORDERS").FindOne(
		db.ctx,
		filter,
		options.FindOne().SetProjection(bson.D{
			{"_id", 0},
			{"O_C_ID", 1},
		})).Decode(&doc)

	if err != nil {
		return 0, err
	}

	return int(doc["O_C_ID"].(int32)), nil
}

func (db *ElasticSearch) UpdateOrders(orderId int, warehouseId int, districtId int, oCarrierId int, deliveryDate time.Time) error {
	var err error

	filter := bson.D{
		{"O_ID", orderId},
		{"O_D_ID", districtId},
		{"O_W_ID", warehouseId},
	}

	r, err := db.C.Collection("ORDERS").UpdateOne(db.ctx,
		filter,
		bson.D{
			{"$set", bson.D{
				{"O_CARRIER_ID", oCarrierId},
				{"ORDER_LINE.$[].OL_DELIVERY_D", deliveryDate},
			}},
		})

	if err != nil {
		return err
	}

	if r.MatchedCount == 0 {
		return fmt.Errorf("UpdateOrders: no documents matched")
	}

	return nil
}

func (db *ElasticSearch) SumOLAmount(orderId int, warehouseId int, districtId int) (float64, error) {
	var err error

	match := bson.D{
		{"$match", bson.D{
			{"O_ID", orderId},
			{"O_D_ID", districtId},
			{"O_W_ID", warehouseId},
		}},
	}

	unwind := bson.D{
		{"$unwind", "$ORDER_LINE"},
	}

	group := bson.D{
		{"$group", bson.D{
			{"_id", "OL_O_ID"},
			{"sumOlAmount", bson.D{
				{"$sum", "$ORDER_LINE.OL_AMOUNT"},
			}},
		}},
	}

	cursor, err := db.C.Collection("ORDERS").Aggregate(db.ctx, mongo.Pipeline{match, unwind, group})
	defer cursor.Close(db.ctx)
	if err != nil {
		return 0, err
	}

	cursor.Next(db.ctx)

	var agg bson.M
	err = cursor.Decode(&agg)
	if err != nil {
		return 0, err
	}

	return float64(agg["sumOlAmount"].(float64)), nil

}

func (db *ElasticSearch) UpdateCustomer(customerId int, warehouseId int, districtId int, sumOlTotal float64) error {
	var err error

	r, err := db.C.Collection("CUSTOMER").UpdateOne(db.ctx,
		bson.D{
			{"C_ID", customerId},
			{"C_D_ID", districtId},
			{"C_W_ID", warehouseId},
		},
		bson.D{
			{"$inc", bson.D{
				{"C_BALANCE", sumOlTotal},
			}},
		},
		nil,
	)

	if err != nil {
		return err
	}

	if r.MatchedCount == 0 {
		return fmt.Errorf("no matched documents")
	}

	return nil
}

func (db *ElasticSearch) GetNextOrderId(warehouseId int, districtId int) (int, error) {

	var oid bson.M
	var query = &bson.D{
		{"D_W_ID", warehouseId},
		{"D_ID", districtId},
	}

	err := db.C.Collection("DISTRICT").FindOne(
		db.ctx,
		query,
		options.FindOne().SetProjection(bson.D{
			{"_id", 0},
			{"D_NEXT_O_ID", 1},
		}).SetComment("STOCK_LEVEL")).Decode(&oid)

	if err != nil {
		return 0, err
	}

	return int(oid["D_NEXT_O_ID"].(int32)), nil
}

func (db *ElasticSearch) GetStockCount(orderIdLt int, orderIdGt int, threshold int, warehouseId int, districtId int) (int64, error) {

	cursor, err := db.C.Collection("ORDERS").Find(db.ctx,
		bson.D{
			{"O_W_ID", warehouseId},
			{"O_D_ID", districtId},
			{"O_ID", bson.D{
				{"$lt", orderIdLt},
				{"$gte", orderIdGt},
			}},
		}, options.Find().SetProjection(bson.D{
			{"ORDER_LINE.OL_I_ID", 1},
		}).SetComment("STOCK_LEVEL"))

	if err != nil {
		return 0, err
	}

	defer cursor.Close(db.ctx)
	var orderIds []int32

	for cursor.Next(db.ctx) {
		var order bson.M
		if err = cursor.Decode(&order); err != nil {
			return 0, err
		}

		for _, value := range order["ORDER_LINE"].(primitive.A) {
			orderIds = append(orderIds, value.(primitive.M)["OL_I_ID"].(int32))
		}
	}

	c, err := db.C.Collection("STOCK").CountDocuments(db.ctx, bson.D{
		{"S_W_ID", warehouseId},
		{"S_I_ID", bson.D{
			{"$in", orderIds},
		}},
		{"S_QUANTITY", bson.D{
			{"$lt", threshold},
		}},
	})

	if err != nil {
		return 0, err
	}

	return c, nil
}

func (db *ElasticSearch) GetCustomerById(customerId int, warehouseId int, districtId int) (*models.Customer, error) {

	var err error
	var customer models.Customer

	projection := bson.D{
		{"_id", 0},
		{"C_ID", 1},
		{"C_FIRST", 1},
		{"C_MIDDLE", 1},
		{"C_LAST", 1},
		{"C_BALANCE", 1},
	}

	err = db.C.Collection("CUSTOMER").FindOne(db.ctx, bson.D{
		{"C_W_ID", warehouseId},
		{"C_D_ID", districtId},
		{"C_ID", customerId},
	}, options.FindOne().SetComment("ORDER_STATUS").SetProjection(projection)).Decode(&customer)

	if err != nil {
		return nil, err
	}

	return &customer, nil
}

func (db *ElasticSearch) GetCustomerByName(name string, warehouseId int, districtId int) (*models.Customer, error) {

	var customer models.Customer

	projection := bson.D{
		{"_id", 0},
		{"C_ID", 1},
		{"C_FIRST", 1},
		{"C_MIDDLE", 1},
		{"C_LAST", 1},
		{"C_BALANCE", 1},
	}

	cursor, err := db.C.Collection("CUSTOMER").Find(db.ctx, bson.D{
		{"C_W_ID", warehouseId},
		{"C_D_ID", districtId},
		{"C_LAST", name},
	}, options.Find().SetProjection(projection))

	defer cursor.Close(db.ctx)

	if err != nil {
		return nil, err
	}

	var customers []models.Customer
	err = cursor.All(db.ctx, &customers)

	if err != nil {
		return nil, err
	}
	if len(customers) == 0 {
		return nil, fmt.Errorf("No customer found")
	}

	i_ := int((len(customers) - 1) / 2)

	customer = customers[i_]

	return &customer, nil
}

func (db *ElasticSearch) GetLastOrder(customerId int, warehouseId int, districtId int) (*models.Order, error) {
	var err error
	var order models.Order

	projection := bson.D{
		{"O_ID", 1},
		{"O_CARRIER_ID", 1},
		{"O_ENTRY_D", 1},
	}

	sort := bson.D{{"O_ID", 1}}

	err = db.C.Collection("ORDERS").FindOne(db.ctx, bson.D{
		{"O_W_ID", warehouseId},
		{"O_D_ID", districtId},
		{"O_C_ID", customerId},
	},
		options.FindOne().SetProjection(projection).SetSort(sort)).Decode(&order)

	if err != nil {
		return nil, err
	}

	return &order, nil
}

func (db *ElasticSearch) GetOrderLines(orderId int, warehouseId int, districtId int) (*[]models.OrderLine, error) {
	var err error
	var order models.Order

	projection := bson.D{
		{"ORDER_LINE", 1},
	}

	err = db.C.Collection("ORDERS").FindOne(db.ctx, bson.D{
		{"O_W_ID", warehouseId},
		{"O_D_ID", districtId},
		{"O_ID", orderId},
	},
		options.FindOne().SetProjection(projection)).Decode(&order)

	if err != nil {
		return nil, err
	}

	return &order.ORDER_LINE, nil
}

func (db *ElasticSearch) GetWarehouse(warehouseId int) (*models.Warehouse, error) {

	var err error

	warehouseProjection := bson.D{
		{"W_NAME", 1},
		{"W_STREET_1", 1},
		{"W_STREET_2", 1},
		{"W_CITY", 1},
		{"W_STATE", 1},
		{"W_ZIP", 1},
	}

	var warehouse models.Warehouse

	err = db.C.Collection("WAREHOUSE").FindOne(db.ctx, bson.D{
		{"W_ID", warehouseId},
	},
		options.FindOne().SetProjection(warehouseProjection),
	).Decode(&warehouse)

	if err != nil {
		return nil, err
	}

	return &warehouse, nil
}

func (db *ElasticSearch) UpdateWarehouseBalance(warehouseId int, amount float64) error {

	r, err := db.C.Collection("WAREHOUSE").UpdateOne(db.ctx, bson.D{
		{"W_ID", warehouseId},
	},
		bson.D{
			{"$inc", bson.D{
				{"W_YTD", amount},
			}},
		},
	)

	if err != nil {
		return err
	}

	if r.MatchedCount == 0 {
		return fmt.Errorf("no warehouse found")
	}

	return nil
}

func (db *ElasticSearch) GetDistrict(warehouseId int, districtId int) (*models.District, error) {
	var err error

	var district models.District

	err = db.C.Collection("DISTRICT").FindOne(db.ctx, bson.D{
		{"D_ID", districtId},
		{"D_W_ID", warehouseId},
	}).Decode(&district)

	if err != nil {
		return nil, err
	}

	return &district, nil
}

func (db *ElasticSearch) UpdateDistrictBalance(warehouseId int, districtId int, amount float64) error {
	filter := bson.D{
		{"D_ID", districtId},
		{"D_W_ID", warehouseId},
	}

	update := bson.D{
		{"$inc", bson.D{
			{"D_YTD", amount},
		}},
	}

	r, err := db.C.Collection("DISTRICT").UpdateOne(db.ctx, filter, update, nil)

	if r.MatchedCount == 0 {
		return fmt.Errorf("No district found")
	}

	if err != nil {
		return err
	}

	return nil
}

func (db *ElasticSearch) InsertHistory(
	warehouseId int,
	districtId int,
	date time.Time,
	amount float64,
	data string,
) error {

	_, err := db.C.Collection("HISTORY").InsertOne(db.ctx, bson.D{
		{"H_D_ID", districtId},
		{"H_W_ID", warehouseId},
		{"H_C_W_ID", warehouseId},
		{"H_C_D_ID", districtId},
		{"H_DATE", date},
		{"H_AMOUNT", amount},
		{"H_DATA", date},
	})

	return err
}

func (db *ElasticSearch) UpdateCredit(customerId int, warehouseId int, districtId int, balance float64, data string) error {
	//updateBCCustomer
	var err error
	update := bson.D{
		{"$inc", bson.D{
			{"C_BALANCE", -1 * balance},
			{"C_YTD_PAYMENT", balance},
			{"C_PAYMENT_CNT", 1},
		}},
	}

	if len(data) > 0 {
		update = append(update, bson.E{"$set", bson.D{
			{"C_DATA", data},
		}})
	}

	_, err = db.C.Collection("CUSTOMER").UpdateOne(db.ctx,
		bson.D{
			{"C_ID", customerId},
			{"C_W_ID", warehouseId},
			{"C_D_ID", districtId},
		},
		update, nil)

	if err != nil {
		return err
	}

	return nil
}

func (db *ElasticSearch) CreateOrder(
	orderId int,
	customerId int,
	warehouseId int,
	districtId int,
	oCarrierId int,
	oOlCnt int,
	allLocal int,
	orderEntryDate time.Time,
	orderLine []models.OrderLine,
) error {

	order := models.Order{
		O_ID:         orderId,
		O_C_ID:       customerId,
		O_D_ID:       districtId,
		O_W_ID:       warehouseId,
		O_ENTRY_D:    orderEntryDate,
		O_CARRIER_ID: oCarrierId,
		O_OL_CNT:     oOlCnt,
		O_ALL_LOCAL:  allLocal,
		ORDER_LINE:   orderLine,
	}

	_, err := db.C.Collection("NEW_ORDER").InsertOne(db.ctx,
		bson.D{
			{"NO_O_ID", orderId},
			{"NO_D_ID", districtId},
			{"NO_W_ID", warehouseId},
		})

	if err != nil {
		return err
	}

	_, err = db.C.Collection("ORDERS").InsertOne(db.ctx, order)

	if err != nil {
		return nil
	}

	return nil
}

//todo: sharding
func (db *ElasticSearch) GetItems(itemIds []int) (*[]models.Item, error) {

	cursor, err := db.C.Collection("ITEM").Find(db.ctx, bson.D{
		{"I_ID", bson.D{
			{"$in", itemIds},
		}}},
		options.Find().SetProjection(bson.D{
			{"_id", 0},
			{"I_ID", 1},
			{"I_PRICE", 1},
			{"I_NAME", 1},
			{"I_DATA", 1},
		}),
	)

	if err != nil {
		return nil, err
	}

	var items []models.Item
	err = cursor.All(db.ctx, &items)

	if err != nil {
		return nil, err
	}

	return &items, nil
}

func (db *ElasticSearch) GetStockInfo(districtId int, iIds []int, iWids []int, allLocal int) (*[]models.Stock, error) {
	var err error
	distCol := fmt.Sprintf("S_DIST_%02d", districtId)
	stockProjection := bson.D{
		{"_id", 0},
		{"S_I_ID", 1},
		{"S_W_ID", 1},
		{"S_QUANTITY", 1},
		{"S_DATA", 1},
		{"S_YTD", 1},
		{"S_ORDER_CNT", 1},
		{"S_REMOTE_CNT", 1},
		{distCol, 1},
	}

	var cursor *mongo.Cursor
	if allLocal == 1 {
		cursor, err = db.C.Collection("STOCK").Find(db.ctx, bson.D{
			{"S_I_ID", bson.D{
				{"$in", iIds},
			}},
			{"S_W_ID", iWids[0]},
		})

		if err != nil {
			return nil, err
		}
	} else {
		var searchList []bson.D
		for item, value := range iIds {
			searchList = append(searchList, bson.D{
				{"S_I_ID", value},
				{"S_W_ID", iWids[item]},
			})
		}

		cursor, err = db.C.Collection("STOCK").Find(db.ctx,
			bson.D{
				{"$or", searchList},
			}, options.Find().SetProjection(stockProjection))

		if err != nil {
			return nil, err
		}
	}

	var stocks []models.Stock

	err = cursor.All(db.ctx, &stocks)
	if err != nil {
		return nil, err
	}

	return &stocks, nil
}

func (db *ElasticSearch) UpdateStock(stockId int, warehouseId int, quantity int, ytd int, ordercnt int, remotecnt int) error {
	ru, err := db.C.Collection("STOCK").UpdateOne(db.ctx,
		bson.D{
			{"S_I_ID", stockId},
			{"S_W_ID", warehouseId},
		},
		bson.D{
			{"$set", bson.D{
				{"S_QUANTITY", quantity},
				{"S_YTD", ytd},
				{"S_ORDER_CNT", ordercnt},
				{"S_REMOTE_CNT", remotecnt},
			}},
		},
	)

	if err != nil {
		return err
	}

	if ru.MatchedCount == 0 {
		return fmt.Errorf("0 document matched")
	}

	return nil
}
