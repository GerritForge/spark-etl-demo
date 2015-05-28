package uk.co.pragmasoft.experiments.bigdata.spark

case class CustomerData(customerId: String, name: String, address: Option[String])

object CustomerData {
   type LineWithErrorDescription = (String, Array[String])

   def fromStringArray(values: Array[String]): Either[LineWithErrorDescription, CustomerData] = {
     if (values.length > 2) {
       Right( CustomerData(values(0), values(1), if(values.length > 2) Some(values(2)) else None ) )
     } else {
       Left( (s"Invalid number of tokens expected at least two", values) )
     }
   }

   implicit def asStringArray(customer: CustomerData): Array[String] = Array(customer.customerId, customer.name, customer.address.getOrElse(null))
 }
