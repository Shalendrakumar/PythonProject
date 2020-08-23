import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

print(spark)
print(sc)

 # sum the salary of employee who is from dept 101

emp = sc.textFile("sp_emp.txt")
fild = emp.filter(lambda d : d.split(",")[3] in('101','deptid'))
fild1 = fild.filter(lambda d : d.split(",")[0] not in('ename'))
filsalary = fild1.map(lambda s : int(s.split(",")[1]))
sumsalary = filsalary.reduce(lambda  x,y : x+y)
print(sumsalary)

#sum of salary department wise

emp = sc.textFile("sp_emp.txt")
empf = emp.filter(lambda d : d.split(",")[3] not in('deptid'))
empg = empf.map(lambda e : (int(e.split(",")[3]) , int(e.split(",")[1])))
empgk = empg.groupByKey()
empgsum = empgk.map(lambda e : (e[0], sum(e[1])))
empgsum1 = empgk.map(lambda e : (e[0], sum(e[1]),max(e[1]),min(e[1])))
for i in empgsum.collect():
    print("deptid = ",i[0],"  and sum of salary = ",i[1])

for i in empgsum1.collect():
    print("deptid = ",i[0]," sum of salary = ",i[1]," Max salary = ",i[2]," Min salary = ",i[3])

print("\nsame work new method")
emp = sc.textFile("sp_emp.txt")
empf = emp.filter(lambda d : d.split(",")[3] not in('deptid'))
empg = empf.map(lambda e : (int(e.split(",")[3]) , int(e.split(",")[1])))
empgsum = empg.reduceByKey(lambda a,b : a+b)
for i in empgsum.collect():
    print("DeptID is = ",i[0],"  and SUM of salary = ",i[1])

print("\nprint sum of salary by using operator")
from operator import add
empgsum1 = empg.reduceByKey(add)
for j in empgsum1.collect():
    print("DeptID is = ",j[0],"  and SUM of salary = ",j[1])

locdist = emp.map(lambda e : e.split(",")[2]).distinct()
print(locdist.collect())

print("\n-----RDD from collection ----")

l = list(range(1,20))
lrdd = sc.parallelize(l)
leven = lrdd.filter(lambda l : l%2==0)
lodd = lrdd.filter(lambda l : l%2!=0)
print("even number ",leven.collect())
print("odd Number ",lodd.collect())

esum = leven.reduce(lambda a,b:a+b)
leven.reduce(add)
lmin = leven.reduce(lambda x,y : x if (x<y) else y)
lmax = leven.reduce(lambda x,y : x if (x>y) else y)
print("\nSum even Number ",esum)
print("Min Number ",lmin)
print("Max Number ",lmax)

print("\n-----Union of RDD---------")
rdd1 = sc.parallelize([1, 2, 3])
rdd2 = sc.parallelize([4, 5, 6])
rdd3 = sc.parallelize([7, 8, 9])
rdd = sc.union([rdd1, rdd2, rdd3])
print(rdd.collect())