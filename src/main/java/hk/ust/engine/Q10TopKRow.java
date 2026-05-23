package hk.ust.engine;

/** One row of the Q10 top-K materialized result. */
public record Q10TopKRow(
    long cCustkey,
    String cName,
    long revenueCents,
    long cAcctbalCents,
    String nName,
    String cAddress,
    String cPhone,
    String cComment) {}
