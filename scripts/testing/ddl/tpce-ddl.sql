-- =========================================
-- DIMENSION TABLES
-- =========================================

-- TPC-E Clause 2.2.8.4
CREATE TABLE zip_code (
   zc_code CHAR(12) NOT NULL,
   zc_town VARCHAR(80) NOT NULL,
   zc_div VARCHAR(80) NOT NULL,
   PRIMARY KEY (zc_code)
);

-- TPC-E Clause 2.2.8.1
CREATE TABLE address (
   ad_id BIGINT NOT NULL,
   ad_line1 VARCHAR(80),
   ad_line2 VARCHAR(80),
   ad_zc_code CHAR(12) NOT NULL REFERENCES zip_code (zc_code),
   ad_ctry VARCHAR(80),
   PRIMARY KEY (ad_id)
);

-- TPC-E Clause 2.2.8.2
CREATE TABLE status_type (
   st_id CHAR(4) NOT NULL,
   st_name CHAR(10) NOT NULL,
   PRIMARY KEY (st_id)
);

-- TPC-E Clause 2.2.8.3
CREATE TABLE taxrate (
   tx_id CHAR(4) NOT NULL,
   tx_name VARCHAR(50) NOT NULL,
   tx_rate FLOAT NOT NULL CHECK (tx_rate >= 0),
   PRIMARY KEY (tx_id)
);

-- =========================================
-- CUSTOMER TABLES (1/2)
-- =========================================

-- TPC-E Clause 2.2.5.2
CREATE TABLE customer (
   c_id BIGINT NOT NULL,
   c_tax_id VARCHAR(20) NOT NULL,
   c_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
   c_l_name VARCHAR(30) NOT NULL,
   c_f_name VARCHAR(30) NOT NULL,
   c_m_name CHAR(1),
   c_gndr CHAR(1),
   c_tier SMALLINT NOT NULL,
   c_dob TIMESTAMP NOT NULL,
   c_ad_id BIGINT NOT NULL REFERENCES address (ad_id),
   c_ctry_1 CHAR(3),
   c_area_1 CHAR(3),
   c_local_1 CHAR(10),
   c_ext_1 CHAR(5),
   c_ctry_2 CHAR(3),
   c_area_2 CHAR(3),
   c_local_2 CHAR(10),
   c_ext_2 CHAR(5),
   c_ctry_3 CHAR(3),
   c_area_3 CHAR(3),
   c_local_3 CHAR(10),
   c_ext_3 CHAR(5),
   c_email_1 VARCHAR(50),
   c_email_2 VARCHAR(50),
   PRIMARY KEY (c_id)
);
CREATE INDEX i_c_tax_id ON customer (c_tax_id);

-- =========================================
-- MARKET TABLES
-- =========================================

-- TPC-E Clause 2.2.7.4
CREATE TABLE exchange (
   ex_id CHAR(6) NOT NULL,
   ex_name VARCHAR(100) NOT NULL,
   ex_num_symb INTEGER NOT NULL,
   ex_open INTEGER NOT NULL,
   ex_close INTEGER NOT NULL,
   ex_desc VARCHAR(150),
   ex_ad_id BIGINT NOT NULL REFERENCES address (ad_id),
   PRIMARY KEY (ex_id)
);

-- TPC-E Clause 2.2.7.10
CREATE TABLE sector (
   sc_id CHAR(2) NOT NULL,
   sc_name VARCHAR(30) NOT NULL,
   PRIMARY KEY (sc_id)
);

-- TPC-E Clause 2.2.7.6
CREATE TABLE industry (
   in_id CHAR(2) NOT NULL,
   in_name VARCHAR(50) NOT NULL,
   in_sc_id CHAR(2) NOT NULL REFERENCES sector (sc_id),
   PRIMARY KEY (in_id)
);

-- TPC-E Clause 2.2.7.1
CREATE TABLE company (
   co_id BIGINT NOT NULL,
   co_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
   co_name VARCHAR(60) NOT NULL,
   co_in_id CHAR(2) NOT NULL REFERENCES industry (in_id),
   co_sp_rate CHAR(4) NOT NULL,
   co_ceo VARCHAR(100) NOT NULL,
   co_ad_id BIGINT NOT NULL REFERENCES address (ad_id),
   co_desc VARCHAR(150) NOT NULL,
   co_open_date TIMESTAMP NOT NULL,
   PRIMARY KEY (co_id)
);
CREATE INDEX i_co_name ON company (co_name);

-- TPC-E Clause 2.2.7.2
CREATE TABLE company_competitor (
   cp_co_id BIGINT NOT NULL REFERENCES company (co_id),
   cp_comp_co_id BIGINT NOT NULL REFERENCES company (co_id),
   cp_in_id CHAR(2) NOT NULL REFERENCES industry (in_id),
   PRIMARY KEY (cp_co_id, cp_comp_co_id, cp_in_id)
);

-- TPC-E Clause 2.2.7.11
CREATE TABLE security (
   s_symb CHAR(15) NOT NULL,
   s_issue CHAR(6) NOT NULL,
   s_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
   s_name VARCHAR(70) NOT NULL,
   s_ex_id CHAR(6) NOT NULL REFERENCES exchange (ex_id),
   s_co_id BIGINT NOT NULL REFERENCES company (co_id),
   s_num_out BIGINT NOT NULL,
   s_start_date TIMESTAMP NOT NULL,
   s_exch_date TIMESTAMP NOT NULL,
   s_pe FLOAT NOT NULL,
   s_52wk_high FLOAT NOT NULL,
   s_52wk_high_date TIMESTAMP NOT NULL,
   s_52wk_low FLOAT NOT NULL,
   s_52wk_low_date TIMESTAMP NOT NULL,
   s_dividend FLOAT NOT NULL,
   s_yield FLOAT NOT NULL,
   PRIMARY KEY (s_symb)
);
CREATE INDEX i_security ON security (s_co_id, s_issue);

-- TPC-E Clause 2.2.7.3
CREATE TABLE daily_market (
   dm_date TIMESTAMP NOT NULL,
   dm_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   dm_close FLOAT NOT NULL,
   dm_high FLOAT NOT NULL,
   dm_low FLOAT NOT NULL,
   dm_vol BIGINT NOT NULL,
   PRIMARY KEY (dm_date, dm_s_symb)
);
CREATE INDEX i_dm_s_symb ON daily_market (dm_s_symb);

-- TPC-E Clause 2.2.7.5
CREATE TABLE financial (
   fi_co_id BIGINT NOT NULL REFERENCES company (co_id),
   fi_year INTEGER NOT NULL,
   fi_qtr SMALLINT NOT NULL,
   fi_qtr_start_date TIMESTAMP NOT NULL,
   fi_revenue FLOAT NOT NULL,
   fi_net_earn FLOAT NOT NULL,
   fi_basic_eps FLOAT NOT NULL,
   fi_dilut_eps FLOAT NOT NULL,
   fi_margin FLOAT NOT NULL,
   fi_inventory FLOAT NOT NULL,
   fi_assets FLOAT NOT NULL,
   fi_liability FLOAT NOT NULL,
   fi_out_basic BIGINT NOT NULL,
   fi_out_dilut BIGINT NOT NULL,
   PRIMARY KEY (fi_co_id, fi_year, fi_qtr)
);

-- TPC-E Clause 2.2.7.7
CREATE TABLE last_trade (
   lt_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   lt_dts TIMESTAMP NOT NULL,
   lt_price FLOAT NOT NULL,
   lt_open_price FLOAT NOT NULL,
   lt_vol BIGINT,
   PRIMARY KEY (lt_s_symb)
);

-- TPC-E Clause 2.2.7.8
-- FIXME: The NI_ITEM field may be either LOB(100000) or
-- LOB_Ref, which is a reference to a LOB(100000) object
-- stored outside the table. 
-- TEXT seems to be simpler to handle
CREATE TABLE news_item (
   ni_id BIGINT NOT NULL,
   ni_headline VARCHAR(80) NOT NULL,
   ni_summary VARCHAR(255) NOT NULL,
   ni_item VARCHAR(1024) NOT NULL,
   ni_dts TIMESTAMP NOT NULL,
   ni_source VARCHAR(30) NOT NULL,
   ni_author VARCHAR(30),
   PRIMARY KEY (ni_id)
);

-- TPC-E Clause 2.2.7.9
CREATE TABLE news_xref (
   nx_ni_id BIGINT NOT NULL REFERENCES news_item (ni_id),
   nx_co_id BIGINT NOT NULL REFERENCES company (co_id),
   PRIMARY KEY (nx_ni_id, nx_co_id)
);

-- =========================================
-- BROKER TABLES
-- =========================================

-- TPC-E Clause 2.2.6.1
CREATE TABLE broker (
   b_id BIGINT NOT NULL,
   b_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
   b_name VARCHAR(100) NOT NULL,
   b_num_trades INTEGER NOT NULL,
   b_comm_total FLOAT NOT NULL,
   PRIMARY KEY (b_id)
);

-- =========================================
-- CUSTOMER TABLES (2/2)
-- =========================================

-- TPC-E Clause 2.2.5.3
CREATE TABLE customer_account (
   ca_id BIGINT NOT NULL,
   ca_b_id BIGINT NOT NULL REFERENCES broker (b_id),
   ca_c_id BIGINT NOT NULL REFERENCES customer (c_id),
   ca_name VARCHAR(50),
   ca_tax_st SMALLINT NOT NULL,
   ca_bal FLOAT NOT NULL,
   PRIMARY KEY (ca_id)
);
CREATE INDEX i_ca_c_id ON customer_account (ca_c_id);

-- TPC-E Clause 2.2.5.1
CREATE TABLE account_permission (
   ap_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
   ap_acl CHAR(4) NOT NULL,
   ap_tax_id VARCHAR(20) NOT NULL,
   ap_l_name VARCHAR(30) NOT NULL,
   ap_f_name VARCHAR(30) NOT NULL,
   PRIMARY KEY (ap_ca_id, ap_tax_id)
);

-- TPC-E Clause 2.2.5.4
CREATE TABLE customer_taxrate (
   cx_tx_id CHAR(4) NOT NULL REFERENCES taxrate (tx_id),
   cx_c_id BIGINT NOT NULL REFERENCES customer (c_id),
   PRIMARY KEY (cx_tx_id, cx_c_id)
);

-- =========================================
-- BROKER TABLES (2/2)
-- =========================================

-- TPC-E Clause 2.2.6.9
CREATE TABLE trade_type (
   tt_id CHAR(3) NOT NULL,
   tt_name CHAR(12) NOT NULL,
   tt_is_sell TINYINT NOT NULL,
   tt_is_mrkt TINYINT NOT NULL,
   PRIMARY KEY (tt_id)
);

-- TPC-E Clause 2.2.6.6
CREATE TABLE trade (
   t_id BIGINT NOT NULL,
   t_dts TIMESTAMP NOT NULL,
   t_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
   t_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
   t_is_cash TINYINT NOT NULL,
   t_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   t_qty INTEGER NOT NULL CHECK (t_qty > 0),
   t_bid_price FLOAT NOT NULL CHECK (t_bid_price > 0),
   t_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
   t_exec_name VARCHAR(64) NOT NULL,
   t_trade_price FLOAT,
   t_chrg FLOAT NOT NULL CHECK (t_chrg >= 0),
   t_comm FLOAT NOT NULL CHECK (t_comm >= 0),
   t_tax FLOAT NOT NULL CHECK (t_tax >= 0),
   t_lifo TINYINT NOT NULL,
   PRIMARY KEY (t_id)
);
CREATE INDEX i_t_st_id ON trade (t_st_id);
CREATE INDEX i_t_ca_id ON trade (t_ca_id);

-- TPC-E Clause 2.2.6.5
CREATE TABLE settlement (
   se_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   se_cash_type VARCHAR(40) NOT NULL,
   se_cash_due_date TIMESTAMP NOT NULL,
   se_amt FLOAT NOT NULL,
   PRIMARY KEY (se_t_id)
);

-- TPC-E Clause 2.2.6.7
CREATE TABLE trade_history (
   th_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   th_dts TIMESTAMP NOT NULL,
   th_st_id CHAR(4) NOT NULL REFERENCES status_type (st_id),
   PRIMARY KEY (th_t_id, th_st_id)
);

-- TPC-E Clause 2.2.5.7
CREATE TABLE holding_summary (
   hs_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
   hs_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   hs_qty INTEGER NOT NULL,
   PRIMARY KEY (hs_ca_id, hs_s_symb)
);

-- TPC-E Clause 2.2.5.5
CREATE TABLE holding (
   h_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   h_ca_id BIGINT NOT NULL,
   h_s_symb CHAR(15) NOT NULL,
   h_dts TIMESTAMP NOT NULL,
   h_price FLOAT NOT NULL CHECK (h_price > 0),
   h_qty INTEGER NOT NULL,
   PRIMARY KEY (h_t_id),
   FOREIGN KEY (h_ca_id, h_s_symb) REFERENCES holding_summary (hs_ca_id, hs_s_symb)
);
CREATE INDEX i_holding ON holding (h_ca_id, h_s_symb);

-- TPC-E Clause 2.2.5.6
CREATE TABLE holding_history (
   hh_h_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   hh_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   hh_before_qty INTEGER NOT NULL,
   hh_after_qty INTEGER NOT NULL,
   PRIMARY KEY (hh_h_t_id, hh_t_id)
);

-- TPC-E Clause 2.2.5.9
CREATE TABLE watch_list (
   wl_id BIGINT NOT NULL,
   wl_c_id BIGINT NOT NULL REFERENCES customer (c_id),
   PRIMARY KEY (wl_id)
);
CREATE INDEX i_wl_c_id ON watch_list (wl_c_id);

-- TPC-E Clause 2.2.5.8
CREATE TABLE watch_item (
   wi_wl_id BIGINT NOT NULL REFERENCES watch_list (wl_id),
   wi_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   PRIMARY KEY (wi_wl_id, wi_s_symb)
);

-- =========================================
-- BROKER TABLES
-- =========================================

-- TPC-E Clause 2.2.6.2
CREATE TABLE cash_transaction (
   ct_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   ct_dts TIMESTAMP NOT NULL,
   ct_amt FLOAT NOT NULL,
   ct_name VARCHAR(100),
   PRIMARY KEY (ct_t_id)
);

-- TPC-E Clause 2.2.6.3
CREATE TABLE charge (
   ch_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
   ch_c_tier SMALLINT,
   ch_chrg FLOAT CHECK (ch_chrg > 0),
   PRIMARY KEY (ch_tt_id, ch_c_tier)
);

-- TPC-E Clause 2.2.6.4
CREATE TABLE commission_rate (
   cr_c_tier SMALLINT NOT NULL,
   cr_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
   cr_ex_id CHAR(6) NOT NULL REFERENCES exchange (ex_id),
   cr_from_qty INTEGER NOT NULL CHECK (cr_from_qty >= 0),
   cr_to_qty INTEGER NOT NULL CHECK (cr_to_qty > cr_from_qty),
   cr_rate FLOAT NOT NULL CHECK (cr_rate >= 0),
   PRIMARY KEY (cr_c_tier, cr_tt_id, cr_ex_id, cr_from_qty)
);

-- TPC-E Clause 2.2.6.8
CREATE TABLE trade_request (
   tr_t_id BIGINT NOT NULL REFERENCES trade (t_id),
   tr_tt_id CHAR(3) NOT NULL REFERENCES trade_type (tt_id),
   tr_s_symb CHAR(15) NOT NULL REFERENCES security (s_symb),
   tr_qty INTEGER NOT NULL CHECK (tr_qty > 0),
   tr_bid_price FLOAT NOT NULL CHECK (tr_bid_price > 0),
   tr_ca_id BIGINT NOT NULL REFERENCES customer_account (ca_id),
   --tr_b_id BIGINT NOT NULL REFERENCES broker (b_id),
   PRIMARY KEY (tr_t_id)
);
CREATE INDEX i_tr_s_symb ON trade_request (tr_s_symb);
