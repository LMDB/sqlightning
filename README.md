SQLite3 ported to use LMDB instead of its original Btree code.

To set the LMDB mapsize in pages, use:

    PRAGMA max_page_count=<integer>;

Using tool/speedtest.tcl in the SQLite source tree, the time to insert 1000 records on my laptop SSD was 22.42 seconds using the original code, and only 1.06 seconds using LMDB. Both tests were run 3 times, with results averaged. The actual runtimes were

    Original    MDB
    23.14        1.07
    22.02        1.05
    22.12        1.08

Tested at version 3.7.7.1.

More recent test results using 3.7.17 are on [pastebin](http://pastebin.com/B5SfEieL), summarized below:

<table>
  <tr><td><th>SQLite</th><th>SQLightning</th></tr>
<tr><td colspan="3">Operation times in microseconds, lower is better</tr>
<tr align="right"><td>Sync Seq Write<td>8175.371<td>6171.233</tr>
<tr align="right"><td>Sync Rand Write<td>8308.706<td>6231.249</tr>
<tr align="right"><td>Seq Write<td>25.587<td>31.778</tr>
<tr align="right"><td>Batch Seq Write<td>7.402<td>7.087</tr>
<tr align="right"><td>Rand Write<td>33.235<td>32.902</tr>
<tr align="right"><td>Batch Rand Write<td>18.847<td>13.754</tr>
<tr align="right"><td>Rand Read<td>22.645<td>7.685</tr>
<tr align="right"><td>Seq Read<td>7.557<td>1.551</tr>
<tr align="right"><td>Rev Seq Read<td>7.456<td>1.531</tr>
</table>
