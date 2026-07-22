import { describe, expect, it } from "vitest";
import { languageForIndustryBucket } from "./industryLanguage";

describe("industry-aware Consumer language", () => {
  it("uses platform language for Microsoft's TaRaShaResearch bucket", () => {
    const language = languageForIndustryBucket("AI Cloud, Data & AI Platform, Model Layer : Hyperscale Cloud & AI Platforms");
    expect(language.lens).toBe("Software and platform engine");
    expect(language.growthQuestion).toContain("platform usage");
  });

  it("uses customer and store language for restaurants", () => {
    const language = languageForIndustryBucket("Consumer Discretionary : Restaurants");
    expect(language.lens).toBe("Consumer demand engine");
    expect(language.costSupport).toContain("stores");
  });

  it("warns that financial-company leverage needs different interpretation", () => {
    const language = languageForIndustryBucket("AI Downstream Economic Outcome Layer : Banks / Consumer Finance / Digital Lending");
    expect(language.lens).toBe("Financial network");
    expect(language.leverageExplanation).toContain("part of the product");
  });

  it("falls back to neutral business language", () => {
    expect(languageForIndustryBucket("Unclassified").growthQuestion).toContain("company’s operating engine");
  });
});
