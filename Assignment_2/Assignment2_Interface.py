
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    #Implement RangeQuery Here.
    cur = openconnection.cursor()

    ##Range  Partition
    cur.execute("Select * from RangeRatingsMetadata;")
    metadata = cur.fetchall()
    for row in metadata:
        min_val = row[1]
        max_val = row[2]
        if not ((ratingMinValue > max_val) or (ratingMaxValue < min_val)):
            cur.execute("select * from RangeRatingsPart" + str(row[0]) +" where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue))
            result = cur.fetchall()
            with open(outputPath, "w") as output_file:
                for res in result:
                    output = "RangeRatingsPart" + str(row[0]) + ", " + str(res[0]) + "," + str(res[1]) + "," + str(res[2])
                    output_file.write(output + "\n")

    ##Round Robin Partition
    cur.execute("Select partitionnum from RoundRobinRatingsMetadata;")
    partition_no = cur.fetchall()[0][0]
    for i in range(0, partition_no):
        cur.execute("select * from RoundRobinRatingsPart" + str(i) + " where rating >= " + str(ratingMinValue) + " and rating <= " + str(ratingMaxValue))
        result = cur.fetchall()
        # print(result)
        with open(outputPath, "a") as output_file:
            for res in result:
                output = "RoundRobinRatingsPart" + str(i) + "," + str(res[0]) + "," + str(res[1]) + "," + str(res[2])
                # print(output)
                output_file.write(output + "\n")


def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    cur = openconnection.cursor()

    ##Range Partition
    cur.execute("select * from RangeRatingsMetadata;")
    metadata = cur.fetchall()
    for row in metadata:
        min_val = row[1]
        max_val = row[2]
        if((row[0] == 0 and ratingValue >= min_val and ratingValue <= max_val) or (row[0] != 0 and ratingValue > min_val and ratingValue <= max_val)):
            cur.execute("select * from RangeRatingsPart" + str(row[0]) + " where rating= "+ str(ratingValue))
            answer = cur.fetchall()
            with open(outputPath, "w") as output_file:
                for ans in answer:
                    output = "RangeRatingsPart" + str(row[0]) + "," + str(ans[0]) + "," + str(ans[1]) + "," + str(ans[2])
                    output_file.write(output + "\n")

    ##Round Robin Partition
    cur.execute("Select partitionnum from RoundRobinRatingsMetadata;")
    partition_no = cur.fetchall()[0][0]
    for i in range(0, partition_no):
        cur.execute("select * from RoundRobinRatingsPart" + str(i) + " where rating= " + str(ratingValue))
        answer = cur.fetchall()
        with open(outputPath, "a") as output_file:
            for ans in answer:
                output = "RoundRobinRatingsPart" + str(i) + str(row[0]) + "," + str(ans[0]) + "," + str(ans[1]) + "," + str(ans[2])
                output_file.write(output + "\n")
