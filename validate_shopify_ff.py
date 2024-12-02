import sys, os
sys.path.append("D:\Job Stuff\Hustle\GoPure\PYTHON\FulfilUtils")

import FULFIL_UTILS
import datetime
from json import dumps
from requests import request, post
from pytz import timezone, utc
from pandas.tseries.offsets import BDay

ff_utils = FULFIL_UTILS.fulfil_utils("prod")

#TODO: test url- remove for prod
#URL_SHOPIFY_ORDERS = ff_utils.url_base_shopify + "/orders.json?fulfillment_status=unfulfilled&created_at_min={}&name=GP1846328&fields=id,created_at,name,fulfillment_status,fulfillments,tags"

URL_SHOPIFY_ORDERS = ff_utils.url_base_shopify + "/orders.json?fulfillment_status=unfulfilled&created_at_min={}&created_at_max={}&fields=id,created_at,name,fulfillment_status,fulfillments,tags"
URL_COUNT_SHOPIFY_ORDERS = ff_utils.url_base_shopify + "/orders/count.json?fulfillment_status=unfulfilled&created_at_min={}"
URL_FULFILLMENT = ff_utils.url_base_shopify + "/orders/{0}/fulfillment_orders.json"
URL_UPDATE_TRACKING = ff_utils.url_base_shopify + "/fulfillments.json"



# =======================
# Get the offset date to filter orders being fetched (days ago, in PST - only business days accounted for)
def get_offset_date(days_offset):
  utc_date = datetime.datetime.now(datetime.timezone.utc)
  pst_tz = timezone('US/Pacific')
  pst_date = utc_date.replace(tzinfo=utc).astimezone(pst_tz)
  offset_date = pst_date - BDay(days_offset)
  formatted_date = offset_date.strftime("%Y-%m-%d") + "T00:00:00-07:00" # Adhere to Shopify's date formatting
  return formatted_date


# =======================
# Generate and return a list of CS's from the given input file
def get_shipment(name):
  payload = dumps({
      "filters": [ ["order_references", "=", name] ],
      "fields": [ "packages", "rec_name", "sales", "warehouse", "state", "tracking_number_blurb" ]
  })
  return ff_utils.send_request("Get shipment", "PUT", ff_utils.url_cs.format("search_read"), ff_utils.headers, payload)


# =======================
# Export tracking to shopify
def export_tracking(tracking_no, carrier, tracking_link, order):
  print(f"tracking number: {tracking_no}, carrier: {carrier}, tracking link: {tracking_link}")
  fulfillment_order_response = request("GET", URL_FULFILLMENT.format(order["id"]), headers=ff_utils.headers)
 
  location_id = None
  line_items_by_fulfillment_order = []
  for shpfy_fulfillment_order in fulfillment_order_response.json()["fulfillment_orders"]:

    is_open = shpfy_fulfillment_order["status"] == "open" if shpfy_fulfillment_order else False
    is_not_digital = shpfy_fulfillment_order["delivery_method"]["method_type"] == "shipping" if shpfy_fulfillment_order["delivery_method"] else False

    if is_open and is_not_digital:
      location_id = shpfy_fulfillment_order["assigned_location_id"]

      fulfillment_order_dict = {}
      fulfillment_order_dict["fulfillment_order_id"] = shpfy_fulfillment_order["id"]
      fulfillment_order_dict["fulfillment_order_line_items"] = []

      for line in shpfy_fulfillment_order["line_items"]:
        line_item_dict = {}
        line_item_dict["id"] = line["id"]
        line_item_dict["quantity"] = line["quantity"]
        fulfillment_order_dict["fulfillment_order_line_items"].append(line_item_dict)
      line_items_by_fulfillment_order.append(fulfillment_order_dict)
    
  if location_id is not None:
    update_tracking_payload = dumps({
      "fulfillment": {
          "notify_customer": "false",
          "location_id": location_id,
          "tracking_info": {
              "url": tracking_link,
              "company": carrier,
              "number": tracking_no
          },
          "line_items_by_fulfillment_order": line_items_by_fulfillment_order}
    }, indent = 4)

    print("Payload: ", update_tracking_payload)
    

    update_tracking_response = request("POST", URL_UPDATE_TRACKING, headers=ff_utils.headers, data=update_tracking_payload)
    
    if str(update_tracking_response) == "<Response [201]>":
      ff_utils.add_logs("Successfully updated tracking for {0}".format(order["name"]))
      ff_utils.log_id(order["id"], "successful_tracking_update.txt")
    else:
      ff_utils.log_id(order["id"], "failed_tracking_update.txt")
      ff_utils.add_logs("ERROR - Failed to update tracking for {0}, {1}".format(order["name"], update_tracking_response.json()))

  else:
    if not is_open:  ff_utils.add_logs("Order {} only has unfulfilled lines with status: closed".format(order["name"]))
    elif not is_not_digital: ff_utils.add_logs("Order {} only has unfulfilled digital items.".format(order["name"]))


# =======================
# Get the tracking id of the given package
def get_package_tracking(id_package):
  payload = dumps({
      "filters": [ [ "id", "=", id_package ] ],
      "fields": [ "tracking_number" ]
  })
  response = ff_utils.send_request("Get package's tracking number", "PUT", ff_utils.url_package.format("search_read"), ff_utils.headers, payload)
  return response[0]["tracking_number"]


# =======================
# Get tracking number string from given tracking ID
def get_tracking_details(id_tracking):
  payload = dumps({
        "filters": [ [ "id", "=", id_tracking ] ],
        "fields": [ "tracking_number", "tracking_url", "carrier_identifier" ]
    })
  return ff_utils.send_request("Get tracking number string", "PUT", ff_utils.url_tracking.format("search_read"), ff_utils.headers, payload)[0]


# =======================
# Get all relevant shopify orders
def fetch_orders():
  list_orders = []
  offset_date_min = get_offset_date(6)
  offset_date_max = get_offset_date(4)

  print(f"datemin: {offset_date_min}, datemax: {offset_date_max}")

  total_orders = ff_utils.send_request("Get orders", "GET", URL_COUNT_SHOPIFY_ORDERS.format(offset_date_min, offset_date_max), ff_utils.headers, "")["count"]
  search_response = ff_utils.send_request_raw("Get orders", "GET", URL_SHOPIFY_ORDERS.format(offset_date_min, offset_date_max), ff_utils.headers, "")

  # Check if there's only one page of 50 orders
  if "Link" not in search_response.headers: #TODO: make "not in"
    order_list = search_response.json()["orders"]
    list_orders.extend(order_list)

  # If > 50 orders from the search, iterate through the paginated results     
  else:
    link_header = search_response.headers["Link"]

    ctr = 0
    while(True):
      order_list = search_response.json()["orders"]
      list_orders.extend(order_list)

      print("Fetching orders {} to {} of {}.".format(ctr, ctr + len(order_list), total_orders))
      ctr += len(order_list)

      # Get the link to the next page (if any) and execute that API call
      if " rel=\"next\"" in link_header:
        if " rel=\"previous\"" in link_header:
          link_raw = link_header.split(", ")[1].split(";")[0]
        else:
          link_raw = link_header.split(";")[0]
        link = link_raw[1:-1].replace("https://", "")
        new_link = link.split("?")[1]
        next_url = URL_SHOPIFY_ORDERS.split("?")[0] + "?" + new_link
        
        search_response = request("GET", next_url, headers=ff_utils.headers)
        if "Link" in search_response.headers: link_header = search_response.headers["Link"]
        print("-------------------------")

      # If no more next page, break loop
      else:
          print("[2]\nEND OF RESULTS")
          break
  return list_orders


# =======================
# MAIN FUNCTION
def main():
  list_orders = fetch_orders()

  for order_obj in list_orders:
    ff_utils.add_logs("Processing Order {}".format(order_obj["name"]))

    # Check if shopify order has "TryNow" tag
    if "trynow" not in order_obj["tags"].lower():
      response = get_shipment(order_obj["name"])
      if response != []:
        ff_cs_obj = response[0]

        if ff_cs_obj["packages"] == []:
          ff_utils.add_logs("Order {} has no packages.".format(order_obj["name"]))
        else:
          ff_utils.add_logs("Order {} has a package.".format(order_obj["name"]))
          ff_utils.log_id(ff_cs_obj["id"], "unfulfilled_cs_with_ff_package.txt")

          tracking_number_id = get_package_tracking(ff_cs_obj["packages"][0])
          
          if tracking_number_id is None:
            ff_utils.add_logs("Order {} does NOT have a tracking number in Fulfil.".format(order_obj["name"]))
          else:
            ff_utils.add_logs("Order {} has a tracking number.".format(order_obj["name"]))
            ff_utils.log_id(ff_cs_obj["id"], "unfulfilled_cs_with_ff_tracking.txt")

            tracking_details = get_tracking_details(tracking_number_id)
            #export_tracking(tracking_details["tracking_number"], tracking_details["carrier_identifier"], tracking_details["tracking_url"], order_obj)
      else:
        ff_utils.add_logs("Order {} has no shipments.".format(order_obj["name"]))
    else:
      ff_utils.add_logs("Order {} has \"TryNow\" tag.".format(order_obj["name"]))











##################################################################################################################################################################

main()