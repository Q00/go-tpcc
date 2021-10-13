package types

import "github.com/Percona-Lab/go-tpcc/tpcc/models"

type ResponseES struct {
	ID            string   `json:"_id"`
	Index         string   `json:"_index"`
	PrimaryTerm   int64    `json:"_primary_term"`
	SeqNo         int64    `json:"_seq_no"`
	Shards        ShardsES `json:"_shards"`
	Type          string   `json:"_type"`
	Version       int64    `json:"_version"`
	ForcedRefresh bool     `json:"forced_refresh"`
	Result        string   `json:"result"`
}

type ShardsES struct {
	Failed     int64 `json:"failed"`
	Skipped    int64 `json:"skipped,omitempty"`
	Successful int64 `json:"successful"`
	Total      int64 `json:"total"`
}

// NEWORDER
type SearchResponseESNOrder struct {
	Shards   ShardsES     `json:"_shards"`
	Hits     HitsESNorder `json:"hits"`
	TimedOut bool         `json:"timed_out"`
	Took     int64        `json:"took"`
}
type HitsESNorder struct {
	Hits     []HitResponseESNOrder `json:"hits"`
	MaxScore float64               `json:"max_score"`
	Total    HitsResponseTotalES   `json:"total"`
}

type HitResponseESNOrder struct {
	ID     string          `json:"_id"`
	Index  string          `json:"_index"`
	Score  float64         `json:"_score"`
	Type   string          `json:"_type"`
	Source models.NewOrder `json:"_source"`
}

//ORDER
type SearchResponseESOrder struct {
	Shards   ShardsES    `json:"_shards"`
	Hits     HitsESOrder `json:"hits"`
	TimedOut bool        `json:"timed_out"`
	Took     int64       `json:"took"`
}

type HitsESOrder struct {
	Hits     []HitResponseESOrder `json:"hits"`
	MaxScore float64              `json:"max_score"`
	Total    HitsResponseTotalES  `json:"total"`
}

type HitResponseESOrder struct {
	ID     string       `json:"_id"`
	Index  string       `json:"_index"`
	Score  float64      `json:"_score"`
	Type   string       `json:"_type"`
	Source models.Order `json:"_source"`
}

// WAREHOUSE
type SearchResponseESWarehouse struct {
	Shards   ShardsES        `json:"_shards"`
	Hits     HitsESWareHouse `json:"hits"`
	TimedOut bool            `json:"timed_out"`
	Took     int64           `json:"took"`
}

type HitsESWareHouse struct {
	Hits     []HitResponseESWarehouse `json:"hits"`
	MaxScore float64                  `json:"max_score"`
	Total    HitsResponseTotalES      `json:"total"`
}

type HitResponseESWarehouse struct {
	ID     string           `json:"_id"`
	Index  string           `json:"_index"`
	Score  float64          `json:"_score"`
	Type   string           `json:"_type"`
	Source models.Warehouse `json:"_source"`
}

// DISTRICT
type SearchResponseESDistrict struct {
	Shards   ShardsES       `json:"_shards"`
	Hits     HitsESDistrict `json:"hits"`
	TimedOut bool           `json:"timed_out"`
	Took     int64          `json:"took"`
}

type HitsESDistrict struct {
	Hits     []HitResponseESDistrict `json:"hits"`
	MaxScore float64                 `json:"max_score"`
	Total    HitsResponseTotalES     `json:"total"`
}

type HitResponseESDistrict struct {
	ID     string          `json:"_id"`
	Index  string          `json:"_index"`
	Score  float64         `json:"_score"`
	Type   string          `json:"_type"`
	Source models.District `json:"_source"`
}

// CUSTOMER
type SearchResponseESCustomer struct {
	Shards   ShardsES       `json:"_shards"`
	Hits     HitsESCustomer `json:"hits"`
	TimedOut bool           `json:"timed_out"`
	Took     int64          `json:"took"`
}

type HitsESCustomer struct {
	Hits     []HitResponseESCustomer `json:"hits"`
	MaxScore float64                 `json:"max_score"`
	Total    HitsResponseTotalES     `json:"total"`
}

type HitResponseESCustomer struct {
	ID     string          `json:"_id"`
	Index  string          `json:"_index"`
	Score  float64         `json:"_score"`
	Type   string          `json:"_type"`
	Source models.Customer `json:"_source"`
}

// Item
type SearchResponseESItem struct {
	Shards   ShardsES   `json:"_shards"`
	Hits     HitsESItem `json:"hits"`
	TimedOut bool       `json:"timed_out"`
	Took     int64      `json:"took"`
}

type HitsESItem struct {
	Hits     []HitResponseESItem `json:"hits"`
	MaxScore float64             `json:"max_score"`
	Total    HitsResponseTotalES `json:"total"`
}

type HitResponseESItem struct {
	ID     string      `json:"_id"`
	Index  string      `json:"_index"`
	Score  float64     `json:"_score"`
	Type   string      `json:"_type"`
	Source models.Item `json:"_source"`
}

// Stock
type SearchResponseESStock struct {
	Shards   ShardsES    `json:"_shards"`
	Hits     HitsESStock `json:"hits"`
	TimedOut bool        `json:"timed_out"`
	Took     int64       `json:"took"`
}

type HitsESStock struct {
	Hits     []HitResponseESStock `json:"hits"`
	MaxScore float64              `json:"max_score"`
	Total    HitsResponseTotalES  `json:"total"`
}

type HitResponseESStock struct {
	ID     string       `json:"_id"`
	Index  string       `json:"_index"`
	Score  float64      `json:"_score"`
	Type   string       `json:"_type"`
	Source models.Stock `json:"_source"`
}

// general
type HitsResponseTotalES struct {
	Relation string `json:"relation"`
	Value    int64  `json:"value"`
}

type BoolMustQuery struct {
	Query struct {
		Bool struct {
			Must Must `json:"filter"`
		} `json:"bool"`
	} `json:"query"`
}

type TermsQuery struct {
	Query struct {
		Bool struct {
			Terms Terms `json:"filter"`
		} `json:"bool"`
	} `json:"query"`
}

type ORQuery struct {
	Query struct {
		Bool struct {
			Filter struct {
				Bool struct {
					Terms []Terms `json:"should"`
				} `json:"bool"`
			} `json:"filter"`
		} `json:"bool"`
	} `json:"query"`
}

type Must []map[string]map[string]interface{}

// type Terms []map[string]map[string][]int
type Terms []interface{}
