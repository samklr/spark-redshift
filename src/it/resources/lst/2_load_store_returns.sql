/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

INSERT INTO store_returns (
    sr_returned_date_sk,
    sr_return_time_sk,
    sr_item_sk,
    sr_customer_sk,
    sr_cdemo_sk,
    sr_hdemo_sk,
    sr_addr_sk,
    sr_store_sk,
    sr_reason_sk,
    sr_ticket_number,
    sr_return_quantity,
    sr_return_amt,
    sr_return_tax,
    sr_return_amt_inc_tax,
    sr_fee,
    sr_return_ship_cost,
    sr_refunded_cash,
    sr_reversed_charge,
    sr_store_credit,
    sr_net_loss
)
VALUES
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 123456789, 1, 10.00, 1.00, 11.00, 0.50, 2.00, 8.00, 1.00, 0.50, 0.50),
    (2, 3, 4, 5, 6, 7, 8, 9, 10, 234567890, 2, 20.00, 2.00, 22.00, 1.00, 3.00, 16.00, 2.00, 1.00, 1.00),
    (3, 4, 5, 6, 7, 8, 9, 10, 11, 345678901, 3, 30.00, 3.00, 33.00, 1.50, 4.00, 24.00, 3.00, 1.50, 1.50),
    (4, 5, 6, 7, 8, 9, 10, 11, 12, 456789012, 4, 40.00, 4.00, 44.00, 2.00, 5.00, 32.00, 4.00, 2.00, 2.00),
    (5, 6, 7, 8, 9, 10, 11, 12, 13, 567890123, 5, 50.00, 5.00, 55.00, 2.50, 6.00, 40.00, 5.00, 2.50, 2.50),
    (6, 7, 8, 9, 10, 11, 12, 13, 14, 678901234, 6, 60.00, 6.00, 66.00, 3.00, 7.00, 48.00, 6.00, 3.00, 3.00),
    (7, 8, 9, 10, 11, 12, 13, 14, 15, 789012345, 7, 70.00, 7.00, 77.00, 3.50, 8.00, 56.00, 7.00, 3.50, 3.50),
    (8, 9, 10, 11, 12, 13, 14, 15, 16, 890123456, 8, 80.00, 8.00, 88.00, 4.00, 9.00, 64.00, 8.00, 4.00, 4.00),
    (9, 10, 11, 12, 13, 14, 15, 16, 17, 901234567, 9, 90.00, 9.00, 99.00, 4.50, 10.00, 72.00, 9.00, 4.50, 4.50),
    (10, 11, 12, 13, 14, 15, 16, 17, 18, 1012345678, 10, 100.00, 10.00, 110.00, 5.00, 11.00, 80.00, 10.00, 5.00, 5.00),
    (11, 12, 13, 14, 15, 16, 17, 18, 19, 1123456789, 11, 110.00, 11.00, 121.00, 5.50, 12.00, 88.00, 11.00, 5.50, 5.50),
    (12, 13, 14, 15, 16, 17, 18, 19, 20, 1234567890, 12, 120.00, 12.00, 132.00, 6.00, 13.00, 96.00, 12.00, 6.00, 6.00),
    (13, 14, 15, 16, 17, 18, 19, 20, 21, 1345678901, 13, 130.00, 13.00, 143.00, 6.50, 14.00, 104.00, 13.00, 6.50, 6.50),
    (14, 15, 16, 17, 18, 19, 20, 21, 22, 1456789012, 14, 140.00, 14.00, 154.00, 7.00, 15.00, 112.00, 14.00, 7.00, 7.00),
    (15, 16, 17, 18, 19, 20, 21, 22, 23, 1567890123, 15, 150.00, 15.00, 165.00, 7.50, 16.00, 120.00, 15.00, 7.50, 7.50),
    (16, 17, 18, 19, 20, 21, 22, 23, 24, 1678901234, 16, 160.00, 16.00, 176.00, 8.00, 17.00, 128.00, 16.00, 8.00, 8.00),
    (17, 18, 19, 20, 21, 22, 23, 24, 25, 1789012345, 17, 170.00, 17.00, 187.00, 8.50, 18.00, 136.00, 17.00, 8.50, 8.50),
    (18, 19, 20, 21, 22, 23, 24, 25, 26, 1890123456, 18, 180.00, 18.00, 198.00, 9.00, 19.00, 144.00, 18.00, 9.00, 9.00),
    (19, 20, 21, 22, 23, 24, 25, 26, 27, 1991234567, 19, 190.00, 19.00, 209.00, 9.50, 20.00, 152.00, 19.00, 9.50, 9.50),
    (20, 21, 22, 23, 24, 25, 26, 27, 28, 2092345678, 20, 200.00, 20.00, 220.00, 10.00, 21.00, 160.00, 20.00, 10.00, 10.00)
;