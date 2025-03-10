# bigDataScenarios
Scenario-1
|id  | Name  |
|----|-------|
|1   |Henry  |
|2   |Smith  |
|3   |Hall   |

scenario - 5
Iput
|emp_id  | name  | dept_id  | salary  |
|--------|-------|----------|---------|
|      1 |     A |        A |  1000000|
|      2 |     B |        A |  2500000|
|      3 |     C |        G |   500000|
|      4 |     D |        G |   800000|
|      5 |     E |        W |  9000000|
|      6 |     F |        W |  2000000|

|dept_id1  | dept_name  |
|----------|------------|
|       A  |       AZURE|
|       G  |       GCP  |
|       W  |       AWS  |

OUTPUT:
|emp_id  | name  | dept_name| salary  |
|--------|-------|----------|---------|
|      1 |     A |     AZURE|  1000000|
|      6 |     F |       AWS|  2000000|
|      3 |     C |       GCP|  500000|
