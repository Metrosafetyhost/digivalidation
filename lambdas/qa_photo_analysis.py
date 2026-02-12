import base64
import json

PHOTO_RULES = """
You are analysing a building exterior photo shown on the front cover of a Fire Risk Assessment report.

Rules:
- Be helpful, but do not invent facts. If something is unclear, say "Not known from the photo".
- Write a single summary suitable for a Salesforce long text field.
- Keep it structured and easy to read (short labelled lines).
- Do NOT output JSON keys other than "summary".
"""

def photo_summary_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "summary": {"type": "string"},
        },
        "required": ["summary"],
    }

class PhotoAnalyzer:
    def __init__(self, oai_client, model: str):
        self.oai = oai_client
        self.model = model

    def analyse_cover_png(self, cover_png_bytes: bytes) -> dict:
        img_b64 = base64.b64encode(cover_png_bytes).decode("utf-8")
        image_url = f"data:image/png;base64,{img_b64}"

        schema = photo_summary_schema()

        resp = self.oai.responses.create(
            model=self.model,
            input=[{
                "role": "user",
                "content": [
                    {"type": "input_text", "text": PHOTO_RULES},
                    {"type": "input_text", "text": (
                    "Write ONE summary string starting with exactly:\n"
                    "\"From the image displayed on the front cover of this report:\"\n\n"
                    "Then answer these questions as short labelled lines, in this exact order.\n"
                    "You MAY make reasonable visual estimates/inferences from the photo (e.g., floors from window rows,\n"
                    "basement from lightwells/areaways/steps down). If you cannot tell, write \"Not known from the photo\".\n"
                    "Do NOT invent details you cannot see.\n\n"
                    "Use exactly these labels:\n"
                    "- Floors visible:\n"
                    "- Basement obvious:\n"
                    "- Estimated height (m):\n"
                    "- Estimated era/year built:\n"
                    "- Main external wall type:\n"
                    "- Other external wall types visible:\n"
                    "- Approx. wall type coverage (%):\n"
                    "- Balconies:\n"
                    "- Balcony materials:\n"
                    "- Materials near openings (and approx distance):\n"
                    "- Materials near escape doors (and approx distance):\n"
                    "- Notes/uncertainties:\n\n"
                    "Guidance:\n"
                    "- For Basement obvious: say Yes/No/Not known from the photo, and add 1 short reason.\n"
                    "- For Estimated height (m): provide a single number estimate if possible, otherwise Not known.\n"
                    "- For Estimated era/year built: give a best-guess range like \"c. 1930s\" or \"c. 2000–2010\" if possible.\n"
                    "- For wall types: use plain descriptive terms if you cannot be specific.\n"
                    "- For coverage: list each visible wall type with an approximate % that totals ~100% of what is visible.\n"
                    "- For distances: give rough distances like \"~0.5m\", \"~1m\", \"touching\".\n"
                )},

                    {"type": "input_image", "image_url": image_url},
                ],
            }],
            text={
                "format": {
                    "name": "dewrra_photo_summary",
                    "type": "json_schema",
                    "schema": schema,
                    "strict": True,
                }
            },
        )
        return json.loads(resp.output_text)
