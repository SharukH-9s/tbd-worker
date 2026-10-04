// ── pdf_invoice.typ ──────────────────────────────────────────────────────────
// Tax invoice template for Turf BD booking confirmations.
// Variables are injected from Rust via sys.inputs (typst-as-lib / derive_typst_intoval).
// Access pattern: #import sys: inputs  →  inputs.field_name

#import sys: inputs

// ── Page & Typography ─────────────────────────────────────────────────────────
#set page(margin: (x: 2cm, y: 2cm), paper: "a4")
#set text(font: ("Inter", "Roboto", "Liberation Sans", "Segoe UI", "Arial", "Libertinus Serif"), size: 10.5pt, fill: rgb("#1e293b"))
#set par(leading: 0.65em)

// ── Convenience Aliases ───────────────────────────────────────────────────────
#let booking_id     = inputs.booking_id
#let contact_name   = inputs.contact_name
#let user_email     = inputs.user_email
#let turf_name      = inputs.turf_name
#let game_name      = inputs.game_name
#let slot_start     = inputs.slot_start
#let total_price    = inputs.total_price
#let paid_amount    = inputs.paid_amount
#let due_amount     = inputs.due_amount
#let payment_status = inputs.payment_status
#let is_partial     = inputs.is_partial   // boolean passed from Rust

// ── Header ────────────────────────────────────────────────────────────────────
#grid(
  columns: (1fr, 1fr),
  align(left + horizon)[
    #text(size: 24pt, weight: "black", fill: rgb("#0f172a"))[
      TURF#text(fill: rgb("#10b981"))[BD]
    ]
  ],
  align(right + horizon)[
    #text(size: 18pt, weight: "bold", fill: rgb("#334155"))[TAX INVOICE]\
    #v(2pt)
    #text(size: 11pt, fill: rgb("#64748b"))[Booking \##booking_id]
  ]
)

#v(4pt)
#line(stroke: 2pt + rgb("#0f172a"), length: 100%)
#v(16pt)

// ── Info Grid (Customer | Venue | Schedule) ───────────────────────────────────
#grid(
  columns: (1fr, 1fr, 1fr),
  gutter: 16pt,
  // Customer Details
  [
    #text(size: 8pt, fill: rgb("#64748b"), weight: "bold")[CUSTOMER DETAILS]
    #v(4pt)
    #text(weight: "bold")[#contact_name]\
    #text(size: 10pt, fill: rgb("#475569"))[#user_email]
  ],
  // Venue & Pitch
  [
    #text(size: 8pt, fill: rgb("#64748b"), weight: "bold")[VENUE & PITCH]
    #v(4pt)
    #text(weight: "bold")[#turf_name]\
    #text(size: 10pt, fill: rgb("#475569"))[#game_name]
  ],
  // Match Schedule + Status Badge
  align(right)[
    #text(size: 8pt, fill: rgb("#64748b"), weight: "bold")[MATCH SCHEDULE]
    #v(4pt)
    #text(weight: "bold")[#slot_start]
    #v(6pt)
    #if is_partial [
      #box(
        fill: rgb("#fffbeb"),
        stroke: 1pt + rgb("#fde68a"),
        radius: 4pt,
        inset: (x: 8pt, y: 4pt)
      )[
        #text(size: 8pt, weight: "bold", fill: rgb("#d97706"))[PARTIALLY PAID]
      ]
    ] else [
      #box(
        fill: rgb("#ecfdf5"),
        stroke: 1pt + rgb("#a7f3d0"),
        radius: 4pt,
        inset: (x: 8pt, y: 4pt)
      )[
        #text(size: 8pt, weight: "bold", fill: rgb("#059669"))[FULLY PAID]
      ]
    ]
  ]
)

#v(20pt)

// ── Line Items Table ──────────────────────────────────────────────────────────
#table(
  columns: (1fr, auto, auto),
  stroke: none,
  inset: (x: 10pt, y: 10pt),
  fill: (col, row) => if row == 0 { rgb("#f8fafc") } else { white },

  // Header row
  table.header(
    [#text(size: 10pt, weight: "bold", fill: rgb("#475569"))[DESCRIPTION]],
    [#text(size: 10pt, weight: "bold", fill: rgb("#475569"))[QTY]],
    [#text(size: 10pt, weight: "bold", fill: rgb("#475569"))[AMOUNT]],
  ),

  // Line item
  [
    #text(weight: "bold")[Slot Reservation]\
    #text(size: 9pt, fill: rgb("#64748b"))[#turf_name — #game_name]
  ],
  [1 Match Slot],
  [BDT #total_price],

  // Total row
  table.hline(stroke: 2pt + rgb("#e2e8f0")),
  [#text(weight: "bold")[Total Slot Price]],
  [],
  [#text(weight: "bold")[BDT #total_price]],

  // Amount paid online
  table.hline(stroke: 1pt + rgb("#e2e8f0")),
  [#text(fill: rgb("#059669"))[Amount Paid Online]],
  [],
  [#text(fill: rgb("#059669"), weight: "bold")[BDT #paid_amount]],

  // Amount due at venue (only shown for partial payments)
  ..if is_partial {(
    table.hline(stroke: 1pt + rgb("#fecaca")),
    [#text(weight: "bold")[Amount Due at Venue]],
    [],
    [#text(fill: rgb("#dc2626"), weight: "bold")[BDT #due_amount]],
  )} else {()},
)

#v(16pt)

// ── Payment Notice ────────────────────────────────────────────────────────────
#if is_partial [
  #block(
    fill: rgb("#fffbeb"),
    stroke: 1pt + rgb("#fef3c7"),
    radius: 6pt,
    inset: 14pt,
    width: 100%,
  )[
    #text(weight: "bold", fill: rgb("#92400e"))[Advance Payment Acknowledged]\
    #v(4pt)
    #text(fill: rgb("#92400e"), size: 10pt)[
      BDT #paid_amount has been paid online. Please settle the remaining balance of
      #text(weight: "bold")[BDT #due_amount] at the venue desk prior to match kickoff.
    ]
  ]
] else [
  #block(
    fill: rgb("#ecfdf5"),
    stroke: 1pt + rgb("#d1fae5"),
    radius: 6pt,
    inset: 14pt,
    width: 100%,
  )[
    #text(weight: "bold", fill: rgb("#065f46"))[Paid in Full]\
    #v(4pt)
    #text(fill: rgb("#065f46"), size: 10pt)[
      This reservation has been completely settled online.
      Please present this invoice at the venue for direct entry.
    ]
  ]
]

#v(1fr)

// ── Footer ────────────────────────────────────────────────────────────────────
#line(stroke: 1pt + rgb("#e2e8f0"), length: 100%)
#v(8pt)
#align(center)[
  #text(size: 9pt, fill: rgb("#94a3b8"))[
    Thank you for booking with Turf BD
    #h(6pt) | #h(6pt)
    support\@turfbd.com
    #h(6pt) | #h(6pt)
    This is a system-generated invoice.
  ]
]
