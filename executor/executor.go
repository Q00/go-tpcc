package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/Percona-Lab/go-tpcc/databases"
	"github.com/Percona-Lab/go-tpcc/tpcc/models"
)

type Executor struct {
	batchSize   int
	data        map[string][]interface{}
	db          databases.Database
	retries     int
	transaction bool
}

const DefaultRetries = 10

func NewExecutor(db databases.Database, batchSize int) (*Executor, error) {

	return &Executor{
		batchSize:   512,
		data:        make(map[string][]interface{}),
		db:          db,
		retries:     DefaultRetries,
		transaction: false,
	}, nil
}

func (e *Executor) ChangeBatchSize(batchSize int) {
	e.batchSize = batchSize
}

func (e *Executor) ChangeRetries(r int) {
	e.retries = r
}

// @TODO@
// Error handling

func (e *Executor) SaveBatch(ctx context.Context, collectionName string, d interface{}) error {
	e.data[collectionName] = append(e.data[collectionName], d)

	if len(e.data[collectionName])%e.batchSize == 0 {
		err := e.db.InsertBatch(ctx, collectionName, e.data[collectionName])
		if err != nil {
			return err
		}
		delete(e.data, collectionName)

	}
	return nil
}

func (e *Executor) Flush(ctx context.Context, collectionName string) error {
	err := e.db.InsertBatch(ctx, collectionName, e.data[collectionName])
	if err != nil {
		return err
	}
	delete(e.data, collectionName)
	return nil
}

func (e *Executor) Save(ctx context.Context, collectionName string, d interface{}) error {
	err := e.db.InsertOne(ctx, collectionName, d)
	if err != nil {
		return err
	}

	return nil
}

func (e *Executor) DoTrxRetries(ctx context.Context, dId int, fn func(ctx context.Context) (context.Context, error)) error {
	var err error

	retries := e.retries

	if !e.transaction {
		retries = 1
	}

	for i := 0; i < retries; i++ {
		err = nil
		if e.transaction {
			err = e.db.StartTrx()
			if err != nil {
				return err
			}
		}
		ctx2 := context.Background()
		ctx2, err = fn(ctx)

		if err != nil {
			if e.transaction {
				e := e.db.RollbackTrx(ctx2)
				if e != nil {
					return e
				}
			}
			break
		}

		if e.transaction {
			err := e.db.CommitTrx(ctx2)
			if err != nil {
				return err
			}
		}
	}

	return err
}

func (e *Executor) DoStockLevelTrx(ctx context.Context, warehouseId int, districtId int, threshold int) error {
	// Do Stock Level never requires a transactions

	noid, err := e.db.GetNextOrderId(ctx, warehouseId, districtId)
	if err != nil {
		return err
	}

	_, err = e.db.GetStockCount(ctx, noid, noid-20, threshold, warehouseId, districtId)

	if err != nil {
		return err
	}

	return nil
}

func (e *Executor) DoDeliveryTrx(ctx context.Context, wId int, oCarrierId int, olDeliveryD time.Time, dId int) error {
	var err error
	for i := 1; i <= dId; i++ {
		err = e.DoTrxRetries(ctx, i, func(ctx context.Context) (context.Context, error) {
			ctx2, err := e.DoDelivery(ctx, wId, oCarrierId, olDeliveryD, i)
			if err != nil {
				return ctx2, err
			}

			return ctx2, err

		})
	}
	return err
}

//todo the order of arguments here is weird
// also the dId passed from the worker is probably utterly wrong
func (e *Executor) DoDelivery(ctx context.Context, wId int, oCarrierId int, olDeliveryD time.Time, dId int) (context.Context, error) {

	no, err := e.db.GetNewOrder(ctx, wId, dId)
	if err != nil {
		return ctx, err
	}

	cid, err := e.db.GetCustomerIdOrder(ctx, no.NO_O_ID, wId, dId)
	if err != nil {
		return ctx, err
	}

	olAmount, err := e.db.SumOLAmount(ctx, no.NO_O_ID, wId, dId)
	if err != nil {
		return ctx, err
	}

	err = e.db.DeleteNewOrder(ctx, no.NO_O_ID, wId, dId)
	if err != nil {
		return ctx, err
	}

	_, err = e.db.GetCustomerIdOrder(ctx, no.NO_O_ID, wId, dId)
	if err != nil {
		return ctx, err
	}

	err = e.db.UpdateOrders(ctx, no.NO_O_ID, wId, dId, oCarrierId, olDeliveryD)
	if err != nil {
		return ctx, err
	}

	err = e.db.UpdateCustomer(ctx, cid, wId, dId, olAmount)
	if err != nil {
		return ctx, err
	}

	return ctx, nil
}

func (e *Executor) DoOrderStatusTrx(ctx context.Context, warehouseId, districtId, cId int, cLast string) error {
	return nil
}

func (e *Executor) DoOrderStatus(ctx context.Context, warehouseId, districtId, cId int, cLast string) error {

	var err error

	if cId > 0 {
		_, err = e.db.GetCustomerById(ctx, cId, warehouseId, districtId)
	} else {
		var customer *models.Customer
		customer, err = e.db.GetCustomerByName(ctx, cLast, warehouseId, districtId)
		if err != nil {
			return err
		}
		cId = customer.C_ID
	}

	if err != nil {
		return err
	}

	order, err := e.db.GetLastOrder(ctx, cId, warehouseId, districtId)

	if err != nil {
		return err
	}

	_, err = e.db.GetOrderLines(ctx, order.O_ID, warehouseId, districtId)

	if err != nil {
		return err
	}

	return nil
}

func (e *Executor) DoPaymentTrx(ctx context.Context, warehouseId, districtId int,
	amount float64,
	cWId, cDId, cId int,
	cLast string,
	hDate time.Time,
	badCredit string,
	cdatalen int) error {
	return nil
}

func (e *Executor) DoPayment(
	ctx context.Context,
	warehouseId, districtId int,
	amount float64,
	cWId, cDId, cId int,
	cLast string,
	hDate time.Time,
	badCredit string,
	cdatalen int,
) error {
	warehouse, err := e.db.GetWarehouse(ctx, warehouseId)

	if err != nil {
		return err
	}

	err = e.db.UpdateWarehouseBalance(ctx, warehouseId, amount)

	if err != nil {
		return err
	}

	district, err := e.db.GetDistrict(ctx, warehouseId, districtId)

	if err != nil {
		fmt.Println(warehouseId, districtId)
		return err
	}

	err = e.db.UpdateDistrictBalance(ctx, warehouseId, districtId, amount)

	if err != nil {
		return err
	}
	var customer *models.Customer
	if cId > 0 {
		customer, err = e.db.GetCustomerById(ctx, cId, warehouseId, districtId)
		if err != nil {
			return err
		}
	} else {
		customer, err = e.db.GetCustomerByName(ctx, cLast, warehouseId, districtId)
		if err != nil {
			return err
		}
		cId = customer.C_ID
		return err
	}

	if err != nil {
		return err
	}

	if customer.C_CREDIT == badCredit {
		var buf string

		buf = fmt.Sprintf("%v %v %v %v %v %v|%v", cId, cDId, cWId, districtId, warehouseId, amount, customer.C_DATA)
		err = e.db.UpdateCredit(ctx, cId, warehouseId, districtId, amount, buf[:cdatalen])

		if err != nil {
			return err
		}

	} else {
		err = e.db.UpdateCredit(ctx, cId, warehouseId, districtId, amount, "")

		if err != nil {
			return err
		}
	}

	hData := fmt.Sprintf("%v    %v", warehouse.W_NAME, district.D_NAME)

	err = e.db.InsertHistory(ctx, warehouseId, districtId, time.Now(), amount, hData)

	if err != nil {
		panic(err)
		panic("here")
		return err
	}

	return nil
}

func (e *Executor) DoNewOrderTrx(ctx context.Context, wId, dId, cId int, oEntryD time.Time, iIds []int, iWids []int, iQtys []int) error {
	var err error
	for i := 1; i <= dId; i++ {
		err = e.DoTrxRetries(ctx, i, func(ctx context.Context) (context.Context, error) {
			ctx2, err := e.DoNewOrder(ctx, wId, dId, cId, oEntryD, iIds, iWids, iQtys)
			if err != nil {
				return ctx2, err
			}

			return ctx2, err

		})
	}
	return err
}

func (e *Executor) DoNewOrder(ctx context.Context, wId, dId, cId int, oEntryD time.Time, iIds []int, iWids []int, iQtys []int) (context.Context, error) {
	var err error
	_, err = e.db.GetWarehouse(ctx, wId)
	if err != nil {
		return ctx, err
	}

	district, err := e.db.GetDistrict(ctx, wId, dId)
	if err != nil {
		return ctx, err
	}

	err = e.db.IncrementDistrictOrderId(ctx, wId, dId)
	if err != nil {
		return ctx, err
	}

	_, err = e.db.GetCustomer(ctx, cId, wId, dId)
	if err != nil {
		return ctx, err
	}

	allLocal := 1
	for _, item := range iWids {
		if item != wId {
			allLocal = 0
			break
		}
	}

	items, err := e.db.GetItems(ctx, iIds)
	if err != nil {
		return ctx, err
	}
	if len(*items) != len(iIds) {
		return ctx, fmt.Errorf("TPCC defines 1%% of neworder gives a wrong itemid, causing rollback. This happens on purpose")
	}

	stocks, err := e.db.GetStockInfo(ctx, dId, iIds, iWids, allLocal)
	if err != nil {
		return ctx, err
	}

	if len(*stocks) != len(iIds) {
		return ctx, err
	}

	var orderLines []models.OrderLine

	for i := 0; i < len(iIds); i++ {
		sQuantity := (*stocks)[i].S_QUANTITY

		if sQuantity >= 10+iQtys[i] {
			sQuantity -= iQtys[0]
		} else {
			sQuantity += 91 - iQtys[0]
		}

		S_REMOTE_CNT := (*stocks)[i].S_REMOTE_CNT

		if iWids[i] != wId {
			S_REMOTE_CNT += 1
		}

		err = e.db.UpdateStock(
			ctx,
			(*stocks)[i].S_I_ID,
			iWids[1],
			sQuantity,
			(*stocks)[i].S_YTD+iQtys[i],
			(*stocks)[i].S_ORDER_CNT+1,
			S_REMOTE_CNT,
		)

		if err != nil {
			return ctx, err
		}

		orderLines = append(orderLines, models.OrderLine{
			OL_O_ID:        district.D_NEXT_O_ID,
			OL_NUMBER:      i + 1,
			OL_I_ID:        iIds[i],
			OL_SUPPLY_W_ID: iWids[i],
			OL_DELIVERY_D:  oEntryD,
			OL_QUANTITY:    iQtys[i],
			OL_AMOUNT:      (*items)[i].I_PRICE * float64(iQtys[i]),
			OL_DIST_INFO:   distCol(dId, &(*stocks)[i]),
		})
	}

	err = e.db.CreateOrder(ctx, district.D_NEXT_O_ID, cId, wId, dId, 0, len(iIds), allLocal, oEntryD, orderLines)
	if err != nil {
		return ctx, err
	}

	return ctx, nil
}

func (e *Executor) CreateIndexes() error {
	return e.db.CreateIndexes()
}

func (e *Executor) CreateSchema() error {
	return e.db.CreateSchema()
}

func distCol(dId int, stock *models.Stock) string {
	switch dId {
	case 1:
		return stock.S_DIST_01
	case 2:
		return stock.S_DIST_02
	case 3:
		return stock.S_DIST_03
	case 4:
		return stock.S_DIST_04
	case 5:
		return stock.S_DIST_05
	case 6:
		return stock.S_DIST_06
	case 7:
		return stock.S_DIST_07
	case 8:
		return stock.S_DIST_08
	case 9:
		return stock.S_DIST_09
	default:
		return stock.S_DIST_10
	}
}
