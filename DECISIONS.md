# Design decisions (Part C)

1. **Indexes.** Unique `employees.emp_code` protects the business identifier. Employee `(department, emp_code)` supports sorted lists; `(department, joined_on)` and `joined_on` support headcount queries. Unique attendance `(emp_code, date)` enforces one daily record and supports monthly lookups. Attendance `(date, emp_code)`, `(date, late_minutes)`, and `(emp_code, punch_in)` support listing, late rankings, and latest punch-out lookup. I skipped standalone indexes that do not lead a contract query; extra indexes add write and storage cost.

2. **Punch-in race.** Both requests can find the employee and try inserting the same key. The unique attendance index accepts one insert; that request returns 201. MongoDB rejects the other with `DuplicateKeyError`, which becomes 409. Correctness does not depend on a check-then-insert.

3. **Ties.** `$rank` assigns tied employees the same competition rank. The API filters after ranking, so it returns everyone tied at the `limit` cutoff, ordered by late minutes and then employee code.

4. **Headcount.** The summary starts with employees who joined by month-end and looks up their logs. Employees without logs remain in the pipeline, count once, and add zero attendance.

5. **100x data.** I would measure query plans, then consider daily analytics rollups and keyset pagination for large exports. Raw attendance would remain authoritative.
