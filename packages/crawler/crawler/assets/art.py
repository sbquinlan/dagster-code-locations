import hashlib

import dagster as dg
import requests
from bs4 import BeautifulSoup
from common.sns import SNSResource  # type: ignore

JEREMY_MIRANDA_URL = "https://jeremymiranda.bigcartel.com/"


@dg.op()
def send_email(
    context: dg.OpExecutionContext, sns: SNSResource, subject: str, message: str
):
    return sns.publish(subject, message)


@dg.job()
def send_email_job():
    send_email()


def _get_artworks():
    resp = requests.get(JEREMY_MIRANDA_URL)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    artworks = []
    product_list = soup.body.section.div.div  # type: ignore
    assert product_list is not None, "Could not find product list"
    for product in product_list.find_all("a", class_="product"):
        link = product["href"]
        sold = "sold" in product["class"]
        title = product.find("div", class_="prod-thumb-name").text
        image = product.find("img", class_="product-list-image").src
        artworks.append({"link": link, "title": title, "image": image, "sold": sold})
    return artworks


def _hash_artworks(artworks):
    names = set(art["title"] for art in artworks)
    names = "".join(sorted(names))
    return hashlib.sha256(names.encode("utf-8")).hexdigest()


@dg.sensor(job=send_email_job)
def jeremy_miranda(context: dg.SensorEvaluationContext):
    cursor = str(context.cursor) if context.cursor else None
    artworks = _get_artworks()
    new_cursor = _hash_artworks(artworks)
    context.log.info(f"Cursor: {cursor}, New Cursor: {new_cursor}")
    context.log.info("\n".join(art["title"] for art in artworks))
    if cursor == new_cursor:
        return dg.SkipReason("No new art.")

    body = "\n".join(
        f"{art['title']} - {art['link']} - {art['sold']}" for art in artworks
    )

    yield dg.RunRequest(
        run_key=new_cursor,
        run_config=dg.RunConfig(
            ops={
                "send_email": {"inputs": {"subject": "New Art Alert!", "message": body}}
            }
        ),
    )
    context.update_cursor(new_cursor)
