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
empf=emp.filter(lambda d : d.split(",")[3] not in('deptid'))
empg=empf.map(lambda e : (int(e.split(",")[3]) , int(e.split(",")[1])))
empgk=empg.groupByKey()
empgsum=empgk.map(lambda e : (e[0], sum(e[1])))
empgsum1=empgk.map(lambda e : (e[0], sum(e[1]),max(e[1]),min(e[1])))
for i in empgsum.collect():
    print("deptid = ",i[0],"  and sum of salary = ",i[1])

for i in empgsum1.collect():
    print("deptid = ",i[0]," sum of salary = ",i[1]," Max salary = ",i[2]," Min salary = ",i[3])