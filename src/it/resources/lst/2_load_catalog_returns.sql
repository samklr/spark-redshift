/*
 * Copyright (c) Microsoft Corporation.
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

INSERT INTO "PUBLIC"."catalog_returns" (
    cr_returned_time_sk,
    cr_item_sk,
    cr_refunded_customer_sk,
    cr_refunded_cdemo_sk,
    cr_refunded_hdemo_sk,
    cr_refunded_addr_sk,
    cr_returning_customer_sk,
    cr_returning_cdemo_sk,
    cr_returning_hdemo_sk,
    cr_returning_addr_sk,
    cr_call_center_sk,
    cr_catalog_page_sk,
    cr_ship_mode_sk,
    cr_warehouse_sk,
    cr_reason_sk,
    cr_order_number,
    cr_return_quantity,
    cr_return_amount,
    cr_return_tax,
    cr_return_amt_inc_tax,
    cr_fee,
    cr_return_ship_cost,
    cr_refunded_cash,
    cr_reversed_charge,
    cr_store_credit,
    cr_net_loss,
    cr_returned_date_sk
) VALUES
(1, 1001, 2001, 3001, 4001, 5001, 6001, 7001, 8001, 9001, 10, 20, 30, 40, 50, 1, 1, 10.00, 0.50, 10.50, 1.00, 5.00, 2.00, 0.00, 1.00, 2.00, 20240101),
(2, 1002, 2002, 3002, 4002, 5002, 6002, 7002, 8002, 9002, 11, 21, 31, 41, 51, 2, 2, 20.00, 1.00, 21.00, 2.00, 10.00, 4.00, 0.00, 2.00, 4.00, 20240102),
(3, 1003, 2003, 3003, 4003, 5003, 6003, 7003, 8003, 9003, 12, 22, 32, 42, 52, 3, 3, 30.00, 1.50, 31.50, 3.00, 15.00, 6.00, 0.00, 3.00, 6.00, 20240103),
(4, 1004, 2004, 3004, 4004, 5004, 6004, 7004, 8004, 9004, 13, 23, 33, 43, 53, 4, 4, 40.00, 2.00, 42.00, 4.00, 20.00, 8.00, 0.00, 4.00, 8.00, 20240104),
(5, 1005, 2005, 3005, 4005, 5005, 6005, 7005, 8005, 9005, 14, 24, 34, 44, 54, 5, 5, 50.00, 2.50, 52.50, 5.00, 25.00, 10.00, 0.00, 5.00, 10.00, 20240105),
(6, 1006, 2006, 3006, 4006, 5006, 6006, 7006, 8006, 9006, 15, 25, 35, 45, 55, 6, 6, 60.00, 3.00, 63.00, 6.00, 30.00, 12.00, 0.00, 6.00, 12.00, 20240106),
(7, 1007, 2007, 3007, 4007, 5007, 6007, 7007, 8007, 9007, 16, 26, 36, 46, 56, 7, 7, 70.00, 3.50, 73.50, 7.00, 35.00, 14.00, 0.00, 7.00, 14.00, 20240107),
(8, 1008, 2008, 3008, 4008, 5008, 6008, 7008, 8008, 9008, 17, 27, 37, 47, 57, 8, 8, 80.00, 4.00, 84.00, 8.00, 40.00, 16.00, 0.00, 8.00, 16.00, 20240108),
(9, 1009, 2009, 3009, 4009, 5009, 6009, 7009, 8009, 9009, 18, 28, 38, 48, 58, 9, 9, 90.00, 4.50, 94.50, 9.00, 45.00, 18.00, 0.00, 9.00, 18.00, 20240109),
(10, 1010, 2010, 3010, 4010, 5010, 6010, 7010, 8010, 9010, 19, 29, 39, 49, 59, 10, 10, 100.00, 5.00, 105.00, 10.00, 50.00, 20.00, 0.00, 10.00, 20.00, 20240110),
(11, 1011, 2011, 3011, 4011, 5011, 6011, 7011, 8011, 9011, 20, 30, 40, 50, 60, 1234567900, 11, 110.00, 5.50, 115.50, 11.00, 55.00, 22.00, 0.00, 11.00, 22.00, 20240111),
(12, 1012, 2012, 3012, 4012, 5012, 6012, 7012, 8012, 9012, 21, 31, 41, 51, 61, 1234567901, 12, 120.00, 6.00, 126.00, 12.00, 60.00, 24.00, 0.00, 12.00, 24.00, 20240112),
(13, 1013, 2013, 3013, 4013, 5013, 6013, 7013, 8013, 9013, 22, 32, 42, 52, 62, 1234567902, 13, 130.00, 6.50, 136.50, 13.00, 65.00, 26.00, 0.00, 13.00, 26.00, 20240113),
(14, 1014, 2014, 3014, 4014, 5014, 6014, 7014, 8014, 9014, 23, 33, 43, 53, 63, 1234567903, 14, 140.00, 7.00, 147.00, 14.00, 70.00, 28.00, 0.00, 14.00, 28.00, 20240114),
(15, 1015, 2015, 3015, 4015, 5015, 6015, 7015, 8015, 9015, 24, 34, 44, 54, 64, 1234567904, 15, 150.00, 7.50, 157.50, 15.00, 75.00, 30.00, 0.00, 15.00, 30.00, 20240115),
(16, 1016, 2016, 3016, 4016, 5016, 6016, 7016, 8016, 9016, 25, 35, 45, 55, 65, 1234567905, 16, 160.00, 8.00, 168.00, 16.00, 80.00, 32.00, 0.00, 16.00, 32.00, 20240116),
(17, 1017, 2017, 3017, 4017, 5017, 6017, 7017, 8017, 9017, 26, 36, 46, 56, 66, 1234567906, 17, 170.00, 8.50, 178.50, 17.00, 85.00, 34.00, 0.00, 17.00, 34.00, 20240117),
(18, 1018, 2018, 3018, 4018, 5018, 6018, 7018, 8018, 9018, 27, 37, 47, 57, 67, 1234567907, 18, 180.00, 9.00, 189.00, 18.00, 90.00, 36.00, 0.00, 18.00, 36.00, 20240118),
(19, 1019, 2019, 3019, 4019, 5019, 6019, 7019, 8019, 9019, 28, 38, 48, 58, 68, 1234567908, 19, 190.00, 9.50, 199.50, 19.00, 95.00, 38.00, 0.00, 19.00, 38.00, 20240119),
(20, 1020, 2020, 3020, 4020, 5020, 6020, 7020, 8020, 9020, 29, 39, 49, 59, 69, 1234567909, 20, 200.00, 10.00, 210.00, 20.00, 100.00, 40.00, 0.00, 20.00, 40.00, 20240120);
