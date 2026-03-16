# PandaPak template-folder approach

These two sample files appear to belong to the same template family:

- `/Users/bssgroup/Downloads/[p2] Product detail content (10-15_3).docx`
- `/Users/bssgroup/Downloads/[p2] Product detail content (10-15_3) (1).docx`

The shared structure is stable enough to manage as one template folder rather than
trying to detect duplicate content on the full document.

## Why one folder per template family works

Both files share these signals:

- `Title` style for the product name
- a URL line immediately below the title
- a `Heading2` hero heading
- a feature section
- a use-case section
- a related-products section with a table
- a supplier/company block
- an FAQ section
- a closing summary paragraph

That means we can:

1. recognize the template once
2. save the template rules
3. apply the same rules to every new product page inside that family

## Recommended folder model

```text
templates/
  pandapak_product_detail_v1/
    template_rules.yaml
    canonical_examples/
      sample_01.docx
      sample_02.docx
    approved_reuse.txt
    faq_intents.yaml
```

## First-time template recognition flow

1. Upload 2-3 representative docs for a new template family.
2. Parse Word styles and heading order.
3. Build a template signature from:
   - heading order
   - list patterns
   - table presence
   - FAQ presence
4. Save the signature and rules in one template folder.
5. For each new submission, match it against existing template signatures.
6. If match score is high enough, assign automatically.
7. If no template matches, send to admin review and create a new folder.

## Suggested section treatment for this template

- `supplier` block: ignore as approved boilerplate
- `related products` table: allow high overlap
- `intro`: medium-strict after removing fact-heavy phrases
- `features`: strict
- `use cases`: strict
- `faq answers`: strict
- `conclusion`: strict

## Product submission flow

1. Writer submits a Google Docs URL or pastes content.
2. System auto-detects the template folder.
3. System strips approved template blocks and fact-heavy spans.
4. New content is compared against all previously approved docs in the same folder.
5. The UI returns:
   - green / yellow / red status
   - top similar documents
   - highlighted risky spans
   - reason codes
6. Writer edits and resubmits until it passes.

## Good operational rule

Do not compare a new document against all content in the company. Compare it against
the same template family first. This keeps the warnings relevant and prevents false
positives from other page types.
