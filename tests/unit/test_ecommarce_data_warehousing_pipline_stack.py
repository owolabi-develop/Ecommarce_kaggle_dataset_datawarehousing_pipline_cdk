import aws_cdk as core
import aws_cdk.assertions as assertions

from ecommarce_data_warehousing_pipline.ecommarce_data_warehousing_pipline_stack import EcommarceDataWarehousingPiplineStack

# example tests. To run these tests, uncomment this file along with the example
# resource in ecommarce_data_warehousing_pipline/ecommarce_data_warehousing_pipline_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = EcommarceDataWarehousingPiplineStack(app, "ecommarce-data-warehousing-pipline")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
