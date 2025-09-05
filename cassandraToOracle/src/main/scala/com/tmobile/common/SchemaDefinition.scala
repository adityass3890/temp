package com.tmobile.common

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ col, _ }
import scala.collection.Seq

object SchemaDefinition extends Enumeration {

  //DualImeiPrimaryImei cassandra table schema
  val dualImeiPrimaryImeiSchema = StructType(StructField("serialnum", StringType, true) ::
    StructField("createdby", StringType, true) ::
    StructField("createddate", DateType, true) ::
    StructField("createdtime", StringType, true) ::
    StructField("csn", StringType, true) ::
    StructField("deviceserialnum", StringType, true) ::
    StructField("filename", StringType, true) ::
    StructField("material", StringType, true) ::
    StructField("oem", StringType, true) ::
    StructField("plant", StringType, true) ::
    StructField("postingdate", DateType, true) ::
    StructField("purchaseorder", StringType, true) ::
    StructField("reference", StringType, true) ::
    StructField("serialnum1", StringType, true) ::
    StructField("shipto", StringType, true) ::
    StructField("simnum", StringType, true) ::
    StructField("soldto", StringType, true) ::
    StructField("tac", StringType, true) ::
    StructField("vendor", StringType, true) ::
    StructField("wifimacid", StringType, true)  :: Nil)

  val serializeddevicehistorySchema = StructType(StructField("serialnumber", StringType, true) ::
    StructField("transactionid", StringType, true) ::
    StructField("ban", StringType, true) ::
    StructField("bringyourowndeviceindicator", StringType, true) ::
    StructField("carrierdetails", StringType, true) ::
    StructField("channel", StringType, true) ::
    StructField("claimnumber", StringType, true) ::
    StructField("company", StringType, true) ::
    StructField("conditionofdevice", StringType, true) ::
    StructField("deliverydate", DateType, true) ::
    StructField("destinationpoint", StringType, true) ::
    StructField("dolineitemid", StringType, true) ::
    StructField("donumber", StringType, true) ::
    StructField("eipdetails", StringType, true) ::
    StructField("enddate", DateType, true) ::
    StructField("eventname", StringType, true) ::
    StructField("externalreference", StringType, true) ::
    StructField("filedate", DateType, true) ::
    StructField("filename", StringType, true) ::
    StructField("filetime", IntegerType, true) ::
    StructField("imsi", StringType, true) ::
    StructField("inventorymovementtype", StringType, true) ::
    StructField("leaseloanind", StringType, true) ::
    StructField("manufacturer", StringType, true) ::
    StructField("mastercartonid", StringType, true) ::
    StructField("materialdetails", StringType, true) ::
    StructField("meiddecimal", StringType, true) ::
    StructField("meidhexadecimal", StringType, true) ::
    StructField("model", StringType, true) ::
    StructField("offerstatus", StringType, true) ::
    StructField("offerupdateddate", DateType, true) ::
    StructField("ordercreateddate", DateType, true) ::
    StructField("orderlineid", StringType, true) ::
    StructField("ordertime", IntegerType, true) ::
    StructField("originalserial", StringType, true) ::
    StructField("originatingsystem", StringType, true) ::
    StructField("owningstore", StringType, true) ::
    StructField("palletid", StringType, true) ::
    StructField("polineitem", StringType, true) ::
    StructField("posnrdetails", StringType, true) ::
    StructField("postingdate", DateType, true) ::
    StructField("postingtime", IntegerType, true) ::
    StructField("programcode", StringType, true) ::
    StructField("purchaseorder", StringType, true) ::
    StructField("quotenumber", IntegerType, true) ::
    StructField("quoteprice", IntegerType, true) ::
    StructField("refurbishedindicator", StringType, true) ::
    StructField("rejectionreason", StringType, true) ::
    StructField("retaillineitem", StringType, true) ::
    StructField("retailtradeinserial", StringType, true) ::
    StructField("returnorderlineitem", StringType, true) ::
    StructField("rmaexpirationdate", DateType, true) ::
    StructField("rmanumber", StringType, true) ::
    StructField("salesorderlineitem", StringType, true) ::
    StructField("secondaryimei", StringType, true) ::
    StructField("serialtype", StringType, true) ::
    StructField("skuidfrom", StringType, true) ::
    StructField("sourcecreationdate", DateType, true) ::
    StructField("sourcepoint", StringType, true) ::
    StructField("sprintfinancialaccountnumber", StringType, true) ::
    StructField("sprintnetworkactivationdate", DateType, true) ::
    StructField("storagelocation", StringType, true) ::
    StructField("subscribernumber", StringType, true) ::
    StructField("systemreference", StringType, true) ::
    StructField("trackingnumber", StringType, true) ::
    StructField("transactionstatus", StringType, true) ::
    StructField("transactiontype", StringType, true) ::
    StructField("vendorcode", StringType, true) :: Nil)

  val deviceeventualinfoSchema = StructType(StructField("serialnumber", StringType, true) ::
    StructField("carrierprovider", StringType, true) ::
    StructField("channel", StringType, true) ::
    StructField("createdby", StringType, true) ::
    StructField("createdon", DateType, true) ::
    StructField("dealercode", StringType, true) ::
    StructField("destinationpoint", StringType, true) ::
    StructField("destinationpointlocation", StringType, true) ::
    StructField("dolineitemid", StringType, true) ::
    StructField("donumber", StringType, true) ::
    StructField("eventdate", DateType, true) ::
    StructField("eventname", StringType, true) ::
    StructField("eventtime", StringType, true) ::
    StructField("lastupdatedby", StringType, true) ::
    StructField("lastupdatedon", TimestampType, true) ::
    StructField("leaseindicator", StringType, true) ::
    StructField("originalskuid", StringType, true) ::
    StructField("owningstore", StringType, true) ::
    StructField("owningstorelocation", StringType, true) ::
    StructField("partnertmoskupendingflag", StringType, true) ::
    StructField("programcode", StringType, true) ::
    StructField("purchaseordernumber", StringType, true) ::
    StructField("rmanumber", StringType, true) ::
    StructField("salesdocumenttype", StringType, true) ::
    StructField("salesoffice", StringType, true) ::
    StructField("salesordercreateddate", DateType, true) ::
    StructField("salesordernumber", StringType, true) ::
    StructField("secondaryimei", StringType, true) ::
    StructField("serialnumtype", StringType, true) ::
    StructField("skuid", StringType, true) ::
    StructField("sourcepoint", StringType, true) ::
    StructField("storeflag", StringType, true) ::
    StructField("trackingnumber", StringType, true) ::
    StructField("transactionid", StringType, true) ::
    StructField("transactionstatus", StringType, true) ::
    StructField("transactiontype", StringType, true) ::
    StructField("transactiontypequalifier", StringType, true) ::
    StructField("vendorcode", StringType, true) ::
    StructField("vendorlocation", StringType, true) :: Nil)

  val serializedmaterialSchema = new StructType()

  val stockonhandSchema = StructType(StructField("storeid", StringType, true) ::
    StructField("inventorystatus", StringType, true) ::
    StructField("skuid", StringType, true) ::
    StructField("activityid", StringType, true) ::
    StructField("channel", StringType, true) ::
    StructField("inventorytype", StringType, true) ::
    StructField("lastupdatedby", StringType, true) ::
    StructField("lastupdateddate", TimestampType, true) ::
    StructField("materialdescription", StringType, true) ::
    StructField("materialset", StringType, true) ::
    StructField("quantity", IntegerType, true) ::
    StructField("solr_query", StringType, true) :: Nil)

  val stocktransferorderSchema = StructType(StructField("stonumber", StringType, true) ::
    StructField("destinationpoint", StringType, true) ::
    StructField("actualreceivedstore", StringType, true) ::
    StructField("boxnumber", StringType, true) ::
    StructField("createdby", StringType, true) ::
    StructField("createddate", TimestampType, true) ::
    StructField("deliverymethod", StringType, true) ::
    StructField("deliveryorderlist", StringType, true) ::
    StructField("frontend", StringType, true) ::
    StructField("lastupdatedby", StringType, true) ::
    StructField("lastupdateddate", TimestampType, true) ::
    StructField("nonserializedmateriallist", StringType, true) ::
    StructField("serializedmateriallist", StringType, true) ::
    StructField("solr_query", StringType, true) ::
    StructField("sourcepoint", StringType, true) ::
    StructField("status", StringType, true) ::
    StructField("stolineitem", StringType, true) ::
    StructField("storelocationidentifier", StringType, true) ::
    StructField("trackingnumberlist", StringType, true) ::
    StructField("transtype", StringType, true) ::
    StructField("varianceindicator", StringType, true) :: Nil)

}