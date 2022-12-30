package org.example.base.stock;

public enum Topics {

    STOCK_VOLUME_BY_COMPANY {
        @Override
        public String toString() {
            return "stock-volume-by-company";
        }
    },

    STOCK_TRANSACTIONS {
        @Override
        public String toString() {
            return "stock-transactions";
        }
    },

    STOCK_PERFORMANCE {
        @Override
        public String toString() {
            return "stock-performance";
        }
    },
    COMPANIES {
        @Override
        public String toString() {
            return "stock-companies";
        }
    },
    CLIENTS {
        @Override
        public String toString() {
            return "stock-clients";
        }
    },
    FINANCIAL_NEWS {
        @Override
        public String toString() {
            return "financial-news";
        }
    },
    CLICK_EVENT {
        @Override
        public String toString() {
            return "click-event";
        }
    },
    CLICK_PERFORMANCE {
        @Override
        public String toString() {
            return "click-performance";
        }
    };

    public String topicName(){
        return this.toString();
    }
}
