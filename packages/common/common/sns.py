import dagster as dg
from pydantic import Field

from common.aws import AWSResource  # type: ignore


class SNSResource(dg.ConfigurableResource):
    aws: dg.ResourceDependency[AWSResource]
    topic_arn: str = Field(description="The ARN of the SNS topic to publish to.")

    def publish(self, subject: str, message: str) -> None:
        self.aws.resource("sns").Topic(self.topic_arn).publish(
            Subject=subject, Message=message
        )
