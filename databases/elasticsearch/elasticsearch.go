package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	types "github.com/Percona-Lab/go-tpcc/databases/elasticsearch/models"
	"github.com/Percona-Lab/go-tpcc/helpers"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
)

type ElasticSearch struct {
	Client *elasticsearch.Client
	lock   bool
}

// connect elasticsearch
func NewElasticSearch(uri string, lock bool) (*ElasticSearch, error) {
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
		lock:   lock,
	}, nil
}

func (db *ElasticSearch) CreateSchema() error {
	return nil
}

// transaction 은 es에서는 version update 변수로 작동하므로 pass

func (db *ElasticSearch) StartTrx() error {
	// progress in wrapped function
	return nil
}

func (db *ElasticSearch) CommitTrx(ctx context.Context) error {
	return nil
}

func (db *ElasticSearch) RollbackTrx(ctx context.Context) error {
	return nil
}

// there is no need about indexing on elasticsearch

func (db *ElasticSearch) CreateIndexes() error {
	var q map[string]interface{}
	var ol map[string]interface{}
	var buf bytes.Buffer

	ol = map[string]interface{}{
		"path_match": "order_line",
		"mapping": map[string]interface{}{
			"type": "nested",
		},
	}

	dt := []map[string]interface{}{}
	dt = append(dt, map[string]interface{}{
		"order_line": ol,
	})

	q = map[string]interface{}{
		"dynamic":           "false",
		"dynamic_templates": dt,
	}
	if err := json.NewEncoder(&buf).Encode(q); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	r := bytes.NewReader(buf.Bytes())
	indexes := [1]string{"orders"}
	req := esapi.IndicesPutMappingRequest{
		Index: indexes[:],
		Body:  r,
	}

	res, err := req.Do(context.Background(), db.Client)
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
		}
	}

	return nil
}

func (db *ElasticSearch) InsertOne(ctx context.Context, tableName string, d interface{}) (err error) {
	// request indexing
	dataJSON, err := json.Marshal(d)
	js := string(dataJSON)

	req := esapi.IndexRequest{
		Index:   strings.ToLower(tableName),
		Body:    strings.NewReader(js),
		Refresh: "true",
	}

	res, err := req.Do(ctx, db.Client)

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
		}
	}

	return nil
}

func (db *ElasticSearch) InsertBatch(ctx context.Context, tableName string, d []interface{}) error {

	retryOnConflit := new(int)
	*retryOnConflit = 3

	// request indexing
	indexer, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  strings.ToLower(tableName),
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
func (db *ElasticSearch) IncrementDistrictOrderId(ctx context.Context, warehouseId int, districtId int) error {
	var doc bytes.Buffer
	documentUp := map[string]interface{}{
		"script": "ctx._source.D_NEXT_O_ID += 1",
	}

	if err := json.NewEncoder(&doc).Encode(documentUp); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	// m := types.Must{}

	// m = append(m, map[string]map[string]interface{}{
	// 	"match": {"D_ID": districtId},
	// })
	// m = append(m, map[string]map[string]interface{}{
	// 	"match": {"D_W_ID": warehouseId},
	// })

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": []map[string]interface{}{
					0: {
						"match": map[string]interface{}{
							"D_ID": districtId,
						},
					},
					1: {
						"match": map[string]interface{}{
							"D_W_ID": warehouseId,
						},
					},
				},
			},
		},
	}

	// var q types.BoolMustQuery

	// q.Query.Bool.Must = m

	//!TODO: check version

	indexes := [1]string{"district"}

	refresh := new(bool)
	*refresh = true
	qString, err := json.Marshal(query)
	if err != nil {
		return err
	}

	req := esapi.UpdateByQueryRequest{
		Index:   indexes[:],
		Body:    &doc,
		Refresh: refresh,
		Pretty:  true,
		Query:   helpers.ReplaceSp(string(qString)),
	}

	res, err := req.Do(ctx, db.Client)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	defer res.Body.Close()

	if res.IsError() {
		log.Printf("[%s] Error update document", res.String())
	} else {
		var r map[string]interface{}
		if errJs := json.NewDecoder(res.Body).Decode(&r); errJs != nil {
			log.Printf("Error parsing the response body: %s", errJs)
		}
	}

	return nil
}

// It also deletes new order, as ElasticSearch can do that lock is set to 0
func (db *ElasticSearch) CheckNewOrder(ctx context.Context, warehouseId int, districtId int) (*models.NewOrder, *string, error) {
	m := types.Must{}
	m = append(m, map[string]map[string]interface{}{
		"match": {"NO_D_ID": districtId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"NO_W_ID": warehouseId},
	})

	var q types.BoolMustQuery

	q.Query.Bool.Must = m

	//!TODO: check version

	indexes := [1]string{"new_order"}

	qString, err := json.Marshal(q)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(qString); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}
	if err != nil {
		return nil, nil, err
	}
	var NewOrder models.NewOrder
	var NOID string
	req := esapi.SearchRequest{
		Index: indexes[:],
		// Query: helpers.ReplaceSp(string(qString)),
		Body: &buf,
	}

	res, err := req.Do(ctx, db.Client)
	if err != nil {
		return nil, nil, err
	}

	defer res.Body.Close()

	if res.IsError() {
		return nil, nil, err
	}

	var r types.SearchResponseESNOrder
	if errJs := json.NewDecoder(res.Body).Decode(&r); errJs != nil {
		return nil, nil, err
	}

	for _, hit := range r.Hits.Hits {
		// log.Printf(" * ID=%s", hit.ID)
		NewOrder = hit.Source
		NOID = hit.ID
		// only one
		break
	}

	return &NewOrder, &NOID, nil

}

// It also deletes new order, as ElasticSearch can do that lock is set to 0
func (db *ElasticSearch) GetNewOrder(ctx context.Context, _ int, _ int) (*models.NewOrder, error) {

	if db.lock {
		// check new order exist. if not exist, still locked
		var NewOrder models.NewOrder
		var NOID string

		ID := ctx.Value("ID")
		for {
			var buf bytes.Buffer
			query := map[string]interface{}{
				"query": map[string]interface{}{
					"ids": map[string]interface{}{
						"values": [1]string{ID.(string)},
					},
				},
			}
			if err := json.NewEncoder(&buf).Encode(query); err != nil {
				log.Fatalf("Error encoding query: %s", err)
			}
			indexes := [1]string{"new_order"}

			req := esapi.SearchRequest{
				Index: indexes[:],
				Body:  &buf,
			}

			res, err := req.Do(ctx, db.Client)

			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				continue
			}

			var r types.SearchResponseESNOrder
			if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
				log.Fatalf("Error parsing the response body: %s", err)
				continue
			}

			for _, hit := range r.Hits.Hits {
				// log.Printf(" * ID=%s", hit.ID)
				NewOrder = hit.Source
				NOID = hit.ID
				break
			}

			v := reflect.ValueOf(NewOrder)
			// check get new_order
			if !v.IsZero() {
				break
			}
		}

		// new order lock
		req := esapi.DeleteRequest{
			Index:      "new_order",
			DocumentID: string(NOID),
		}

		// delete
		_, err := req.Do(ctx, db.Client)
		if err != nil {
			return nil, err
		}

		return &NewOrder, nil
	}

	return nil, nil
}

func (db *ElasticSearch) DeleteNewOrder(ctx context.Context, orderId int, warehouseId int, districtId int) error {
	if db.lock {
		return nil
	}
	return nil
}

func (db *ElasticSearch) GetCustomer(ctx context.Context, customerId int, warehouseId int, districtId int) (*models.Customer, error) {
	var customer models.Customer
	m := types.Must{}
	m = append(m, map[string]map[string]interface{}{
		"match": {"C_ID": customerId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"C_D_ID": districtId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"C_W_ID": warehouseId},
	})

	var q types.BoolMustQuery

	q.Query.Bool.Must = m

	//!TODO: check version

	indexes := [1]string{"customer"}

	qString, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: indexes[:],
		Query: helpers.ReplaceSp(string(qString)),
	}

	res, err := req.Do(ctx, db.Client)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, err
	}

	var r types.SearchResponseESCustomer
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return nil, err
	}

	for _, hit := range r.Hits.Hits {
		// log.Printf(" * ID=%s", hit.ID)
		customer = hit.Source
		break
	}

	return &customer, nil
}

// GetCId
func (db *ElasticSearch) GetCustomerIdOrder(ctx context.Context, orderId int, warehouseId int, districtId int) (int, error) {

	m := types.Must{}
	m = append(m, map[string]map[string]interface{}{
		"match": {"O_ID": orderId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"O_D_ID": districtId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"O_W_ID": warehouseId},
	})

	var q types.BoolMustQuery

	q.Query.Bool.Must = m

	//!TODO: check version

	indexes := [1]string{"orders"}

	qString, err := json.Marshal(q)
	if err != nil {
		return 0, err
	}
	var CID int

	req := esapi.SearchRequest{
		Index: indexes[:],
		Query: helpers.ReplaceSp(string(qString)),
	}

	res, err := req.Do(ctx, db.Client)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, err
	}

	var r types.SearchResponseESOrder
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return 0, err
	}

	for _, hit := range r.Hits.Hits {
		// log.Printf(" * ID=%s", hit.ID)
		CID = hit.Source.O_C_ID
		break
	}

	return CID, nil

	// var err error

	// filter := bson.D{
	// 	{"O_ID", orderId},
	// 	{"O_D_ID", districtId},
	// 	{"O_W_ID", warehouseId},
	// }

	// var doc bson.M
	// err = db.C.Collection("orders").FindOne(
	// 	db.ctx,
	// 	filter,
	// 	options.FindOne().SetProjection(bson.D{
	// 		{"_id", 0},
	// 		{"O_C_ID", 1},
	// 	})).Decode(&doc)

	// if err != nil {
	// 	return 0, err
	// }

	// return int(doc["O_C_ID"].(int32)), nil
}

func (db *ElasticSearch) UpdateOrders(ctx context.Context, orderId int, warehouseId int, districtId int, oCarrierId int, deliveryDate time.Time) error {
	// var err error

	// filter := bson.D{
	// 	{"O_ID", orderId},
	// 	{"O_D_ID", districtId},
	// 	{"O_W_ID", warehouseId},
	// }

	// r, err := db.C.Collection("orders").UpdateOne(db.ctx,
	// 	filter,
	// 	bson.D{
	// 		{"$set", bson.D{
	// 			{"O_CARRIER_ID", oCarrierId},
	// 			{"order_line.$[].OL_DELIVERY_D", deliveryDate},
	// 		}},
	// 	})

	// if err != nil {
	// 	return err
	// }

	// if r.MatchedCount == 0 {
	// 	return fmt.Errorf("Updateorders: no documents matched")
	// }

	return nil
}

func (db *ElasticSearch) SumOLAmount(ctx context.Context, orderId int, warehouseId int, districtId int) (float64, error) {
	// aggreagate orderline of deleted order
	return 0, nil
}

func (db *ElasticSearch) UpdateCustomer(ctx context.Context, customerId int, warehouseId int, districtId int, sumOlTotal float64) error {
	// var err error

	// r, err := db.C.Collection("CUSTOMER").UpdateOne(db.ctx,
	// 	bson.D{
	// 		{"C_ID", customerId},
	// 		{"C_D_ID", districtId},
	// 		{"C_W_ID", warehouseId},
	// 	},
	// 	bson.D{
	// 		{"$inc", bson.D{
	// 			{"C_BALANCE", sumOlTotal},
	// 		}},
	// 	},
	// 	nil,
	// )

	// if err != nil {
	// 	return err
	// }

	// if r.MatchedCount == 0 {
	// 	return fmt.Errorf("no matched documents")
	// }

	return nil
}

func (db *ElasticSearch) GetNextOrderId(ctx context.Context, warehouseId int, districtId int) (int, error) {

	// var oid bson.M
	// var query = &bson.D{
	// 	{"D_W_ID", warehouseId},
	// 	{"D_ID", districtId},
	// }

	// err := db.C.Collection("district").FindOne(
	// 	db.ctx,
	// 	query,
	// 	options.FindOne().SetProjection(bson.D{
	// 		{"_id", 0},
	// 		{"D_NEXT_O_ID", 1},
	// 	}).SetComment("stock_LEVEL")).Decode(&oid)

	// if err != nil {
	// 	return 0, err
	// }

	// return int(oid["D_NEXT_O_ID"].(int32)), nil
	return 0, nil
}

func (db *ElasticSearch) GetStockCount(ctx context.Context, orderIdLt int, orderIdGt int, threshold int, warehouseId int, districtId int) (int64, error) {

	// cursor, err := db.C.Collection("orders").Find(db.ctx,
	// 	bson.D{
	// 		{"O_W_ID", warehouseId},
	// 		{"O_D_ID", districtId},
	// 		{"O_ID", bson.D{
	// 			{"$lt", orderIdLt},
	// 			{"$gte", orderIdGt},
	// 		}},
	// 	}, options.Find().SetProjection(bson.D{
	// 		{"order_line.OL_I_ID", 1},
	// 	}).SetComment("stock_LEVEL"))

	// if err != nil {
	// 	return 0, err
	// }

	// defer cursor.Close(db.ctx)
	// var orderIds []int32

	// for cursor.Next(db.ctx) {
	// 	var order bson.M
	// 	if err = cursor.Decode(&order); err != nil {
	// 		return 0, err
	// 	}

	// 	for _, value := range order["order_line"].(primitive.A) {
	// 		orderIds = append(orderIds, value.(primitive.M)["OL_I_ID"].(int32))
	// 	}
	// }

	// c, err := db.C.Collection("stock").CountDocuments(db.ctx, bson.D{
	// 	{"S_W_ID", warehouseId},
	// 	{"S_I_ID", bson.D{
	// 		{"$in", orderIds},
	// 	}},
	// 	{"S_QUANTITY", bson.D{
	// 		{"$lt", threshold},
	// 	}},
	// })

	// if err != nil {
	// 	return 0, err
	// }

	// return c, nil
	return 0, nil
}

func (db *ElasticSearch) GetCustomerById(ctx context.Context, customerId int, warehouseId int, districtId int) (*models.Customer, error) {
	var customer models.Customer
	m := types.Must{}

	m = append(m, map[string]map[string]interface{}{
		"match": {"C_ID": customerId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"C_D_ID": districtId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"C_W_ID": warehouseId},
	})
	var q types.BoolMustQuery

	q.Query.Bool.Must = m

	//!TODO: check version

	indexes := [1]string{"customer"}

	qString, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: indexes[:],
		Query: helpers.ReplaceSp(string(qString)),
	}

	res, err := req.Do(ctx, db.Client)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, err
	}

	var r types.SearchResponseESCustomer
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return nil, err
	}

	for _, hit := range r.Hits.Hits {
		// log.Printf(" * ID=%s", hit.ID)
		customer = hit.Source
		break
	}

	return &customer, nil
}

func (db *ElasticSearch) GetCustomerByName(ctx context.Context, name string, warehouseId int, districtId int) (*models.Customer, error) {

	var customer models.Customer

	// projection := bson.D{
	// 	{"_id", 0},
	// 	{"C_ID", 1},
	// 	{"C_FIRST", 1},
	// 	{"C_MIDDLE", 1},
	// 	{"C_LAST", 1},
	// 	{"C_BALANCE", 1},
	// }

	// cursor, err := db.C.Collection("CUSTOMER").Find(db.ctx, bson.D{
	// 	{"C_W_ID", warehouseId},
	// 	{"C_D_ID", districtId},
	// 	{"C_LAST", name},
	// }, options.Find().SetProjection(projection))

	// defer cursor.Close(db.ctx)

	// if err != nil {
	// 	return nil, err
	// }

	// var customers []models.Customer
	// err = cursor.All(db.ctx, &customers)

	// if err != nil {
	// 	return nil, err
	// }
	// if len(customers) == 0 {
	// 	return nil, fmt.Errorf("No customer found")
	// }

	// i_ := int((len(customers) - 1) / 2)

	// customer = customers[i_]

	return &customer, nil
}

func (db *ElasticSearch) GetLastOrder(ctx context.Context, customerId int, warehouseId int, districtId int) (*models.Order, error) {
	// var err error
	var order models.Order

	// projection := bson.D{
	// 	{"O_ID", 1},
	// 	{"O_CARRIER_ID", 1},
	// 	{"O_ENTRY_D", 1},
	// }

	// sort := bson.D{{"O_ID", 1}}

	// err = db.C.Collection("orders").FindOne(db.ctx, bson.D{
	// 	{"O_W_ID", warehouseId},
	// 	{"O_D_ID", districtId},
	// 	{"O_C_ID", customerId},
	// },
	// 	options.FindOne().SetProjection(projection).SetSort(sort)).Decode(&order)

	// if err != nil {
	// 	return nil, err
	// }

	return &order, nil
}

func (db *ElasticSearch) GetOrderLines(ctx context.Context, orderId int, warehouseId int, districtId int) (*[]models.OrderLine, error) {
	// var err error
	var order models.Order

	// projection := bson.D{
	// 	{"order_line", 1},
	// }

	// err = db.C.Collection("orders").FindOne(db.ctx, bson.D{
	// 	{"O_W_ID", warehouseId},
	// 	{"O_D_ID", districtId},
	// 	{"O_ID", orderId},
	// },
	// 	options.FindOne().SetProjection(projection)).Decode(&order)

	// if err != nil {
	// 	return nil, err
	// }

	return &order.ORDER_LINE, nil
}

func (db *ElasticSearch) GetWarehouse(ctx context.Context, warehouseId int) (*models.Warehouse, error) {

	var err error
	var warehouse models.Warehouse

	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"W_ID": warehouseId,
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}
	indexes := [1]string{"warehouse"}

	req := esapi.SearchRequest{
		Index: indexes[:],
		Query: helpers.ReplaceSp(buf.String()),
	}

	res, err := req.Do(ctx, db.Client)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, err
	}

	var r types.SearchResponseESWarehouse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return nil, err
	}

	for _, hit := range r.Hits.Hits {
		// log.Printf(" * ID=%s", hit.ID)
		warehouse = hit.Source
		break
	}

	return &warehouse, nil
}

func (db *ElasticSearch) UpdateWarehouseBalance(ctx context.Context, warehouseId int, amount float64) error {

	// r, err := db.C.Collection("warehouse").UpdateOne(db.ctx, bson.D{
	// 	{"W_ID", warehouseId},
	// },
	// 	bson.D{
	// 		{"$inc", bson.D{
	// 			{"W_YTD", amount},
	// 		}},
	// 	},
	// )

	// if err != nil {
	// 	return err
	// }

	// if r.MatchedCount == 0 {
	// 	return fmt.Errorf("no warehouse found")
	// }

	return nil
}

func (db *ElasticSearch) GetDistrict(ctx context.Context, warehouseId int, districtId int) (*models.District, error) {

	var err error
	var district models.District

	m := types.Must{}
	m = append(m, map[string]map[string]interface{}{
		"match": {"D_ID": district},
	})

	m = append(m, map[string]map[string]interface{}{
		"match": {"D_W_ID": warehouseId},
	})

	var q types.BoolMustQuery
	q.Query.Bool.Must = m

	indexes := [1]string{"district"}

	qString, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: indexes[:],
		Query: helpers.ReplaceSp(string(qString)),
	}

	res, err := req.Do(ctx, db.Client)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, err
	}

	var r types.SearchResponseESDistrict
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return nil, err
	}

	for _, hit := range r.Hits.Hits {
		// log.Printf(" * ID=%s", hit.ID)
		district = hit.Source
		break
	}

	return &district, nil
}

func (db *ElasticSearch) UpdateDistrictBalance(ctx context.Context, warehouseId int, districtId int, amount float64) error {
	// filter := bson.D{
	// 	{"D_ID", districtId},
	// 	{"D_W_ID", warehouseId},
	// }

	// update := bson.D{
	// 	{"$inc", bson.D{
	// 		{"D_YTD", amount},
	// 	}},
	// }

	// r, err := db.C.Collection("district").UpdateOne(db.ctx, filter, update, nil)

	// if r.MatchedCount == 0 {
	// 	return fmt.Errorf("No district found")
	// }

	// if err != nil {
	// 	return err
	// }

	return nil
}

func (db *ElasticSearch) InsertHistory(ctx context.Context,
	warehouseId int,
	districtId int,
	date time.Time,
	amount float64,
	data string,
) error {

	// _, err := db.C.Collection("HISTORY").InsertOne(db.ctx, bson.D{
	// 	{"H_D_ID", districtId},
	// 	{"H_W_ID", warehouseId},
	// 	{"H_C_W_ID", warehouseId},
	// 	{"H_C_D_ID", districtId},
	// 	{"H_DATE", date},
	// 	{"H_AMOUNT", amount},
	// 	{"H_DATA", date},
	// })

	// return err
	return nil
}

func (db *ElasticSearch) UpdateCredit(ctx context.Context, customerId int, warehouseId int, districtId int, balance float64, data string) error {
	//updateBCCustomer
	// var err error
	// update := bson.D{
	// 	{"$inc", bson.D{
	// 		{"C_BALANCE", -1 * balance},
	// 		{"C_YTD_PAYMENT", balance},
	// 		{"C_PAYMENT_CNT", 1},
	// 	}},
	// }

	// if len(data) > 0 {
	// 	update = append(update, bson.E{"$set", bson.D{
	// 		{"C_DATA", data},
	// 	}})
	// }

	// _, err = db.C.Collection("CUSTOMER").UpdateOne(db.ctx,
	// 	bson.D{
	// 		{"C_ID", customerId},
	// 		{"C_W_ID", warehouseId},
	// 		{"C_D_ID", districtId},
	// 	},
	// 	update, nil)

	// if err != nil {
	// 	return err
	// }

	return nil
}

func (db *ElasticSearch) CreateOrder(ctx context.Context,
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

	no := models.NewOrder{
		NO_O_ID: orderId,
		NO_D_ID: districtId,
		NO_W_ID: warehouseId,
	}

	err := db.InsertOne(ctx, "new_order", no)

	if err != nil {
		return err
	}

	err = db.InsertOne(ctx, "orders", order)

	if err != nil {
		return nil
	}

	return nil
}

//todo: sharding
func (db *ElasticSearch) GetItems(ctx context.Context, itemIds []int) (*[]models.Item, error) {

	var items []models.Item
	var t types.Terms
	t = append(t, map[string]map[string][]int{"terms": {"I_ID": itemIds}})

	var q types.TermsQuery
	q.Query.Bool.Terms = t

	indexes := [1]string{"item"}

	qString, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}

	req := esapi.SearchRequest{
		Index: indexes[:],
		Query: helpers.ReplaceSp(string(qString)),
	}

	res, err := req.Do(ctx, db.Client)

	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, err
	}

	var r types.SearchResponseESItem
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return nil, err
	}

	for _, hit := range r.Hits.Hits {
		// log.Printf(" * ID=%s", hit.ID)
		items = append(items, hit.Source)
	}
	return &items, nil
}

func (db *ElasticSearch) GetStockInfo(ctx context.Context, districtId int, iIds []int, iWids []int, allLocal int) (*[]models.Stock, error) {
	distCol := fmt.Sprintf("S_DIST_%02d", districtId)
	indexes := [1]string{"stock"}

	stockProjection := [8]string{"S_I_ID", "S_W_ID", "S_QUANTITY", "S_DATA", "S_YTD", "S_ORDER_CNT", "S_REMOTE_CNT", distCol}

	// stockProjection := bson.D{
	// 	{"_id", 0},
	// 	{"S_I_ID", 1},
	// 	{"S_W_ID", 1},
	// 	{"S_QUANTITY", 1},
	// 	{"S_DATA", 1},
	// 	{"S_YTD", 1},
	// 	{"S_ORDER_CNT", 1},
	// 	{"S_REMOTE_CNT", 1},
	// 	{distCol, 1},
	// }

	var stocks []models.Stock
	// var cursor *mongo.Cursor
	if allLocal == 1 {
		var t types.Terms
		t = append(t, map[string]map[string][]int{"terms": {"S_I_ID": iIds}})
		t = append(t, map[string]map[string]int{"term": {"S_W_ID": iWids[0]}})

		var q types.TermsQuery
		q.Query.Bool.Terms = t

		qString, err := json.Marshal(q)
		if err != nil {
			return nil, err
		}

		req := esapi.SearchRequest{
			Source: stockProjection[:],
			Index:  indexes[:],
			Query:  helpers.ReplaceSp(string(qString)),
		}

		res, err := req.Do(ctx, db.Client)

		if err != nil {
			log.Fatalf("Error getting response: %s", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, err
		}

		var r types.SearchResponseESStock
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
			return nil, err
		}

		for _, hit := range r.Hits.Hits {
			// log.Printf(" * ID=%s", hit.ID)
			stocks = append(stocks, hit.Source)
		}
	} else {
		var searchList []types.Terms
		for item, value := range iIds {
			var t types.Terms
			t = append(t, map[string]map[string]int{"terms": {"S_I_ID": value}})
			t = append(t, map[string]map[string]int{"terms": {"S_W_ID": iWids[item]}})
			searchList = append(searchList, t)
		}

		var o types.ORQuery
		qString, err := json.Marshal(o)
		if err != nil {
			return nil, err
		}

		o.Query.Bool.Filter.Bool.Terms = searchList

		req := esapi.SearchRequest{
			Source: stockProjection[:],
			Index:  indexes[:],
			Query:  helpers.ReplaceSp(string(qString)),
		}

		res, err := req.Do(ctx, db.Client)

		if err != nil {
			log.Fatalf("Error getting response: %s", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, err
		}

		var r types.SearchResponseESStock
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
			return nil, err
		}

		for _, hit := range r.Hits.Hits {
			// log.Printf(" * ID=%s", hit.ID)
			stocks = append(stocks, hit.Source)
		}
	}

	return &stocks, nil
}

func (db *ElasticSearch) UpdateStock(ctx context.Context, stockId int, warehouseId int, quantity int, ytd int, ordercnt int, remotecnt int) error {
	var doc bytes.Buffer
	s := fmt.Sprintf("ctx._source.S_QUANTITY=%d;ctx._source.S_YTD=%d;ctx._source.S_ORDER_CNT=%d;ctx._source.S_REMOTE_CNT=%d;", quantity, ytd, ordercnt, remotecnt)
	documentUp := map[string]interface{}{
		"script": s,
	}

	if err := json.NewEncoder(&doc).Encode(documentUp); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	m := types.Must{}
	m = append(m, map[string]map[string]interface{}{
		"match": {"S_I_ID": stockId},
	})
	m = append(m, map[string]map[string]interface{}{
		"match": {"S_W_ID": warehouseId},
	})

	var q types.BoolMustQuery

	q.Query.Bool.Must = m

	//!TODO: check version

	indexes := [1]string{"stock"}

	refresh := new(bool)
	*refresh = true

	qString, err := json.Marshal(q)
	if err != nil {
		return err
	}

	req := esapi.UpdateByQueryRequest{
		Index:   indexes[:],
		Body:    &doc,
		Refresh: refresh,
		Pretty:  true,
		Query:   helpers.ReplaceSp(string(qString)),
	}

	res, err := req.Do(ctx, db.Client)
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
