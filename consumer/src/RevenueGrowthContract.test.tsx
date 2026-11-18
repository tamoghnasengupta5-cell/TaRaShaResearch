import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import { revenueOfferingStory } from "../functions/dataProvider";
import { HowItMakesMoney } from "./CompanyStoryHome";
import type { Company } from "./types";
const display=(value:string)=>({value,unit:"USD millions"});
const source={source_url:"https://www.sec.gov/Archives/edgar/data/1/000000000126000001/report.xml",accession:"0000000001-26-000001",unit:"USD",concept:"Revenues"};
function company():Company {
 const growth={cagr_years:3,three_year_prior_ttm:{sources:[source]},three_year_cagr_percent:{reason_text:null}};
 const story=revenueOfferingStory({offerings:[{id:"srt:ProductOrServiceAxis/Product",label:"Product",history:{},ttm:{period_end:"2026-06-30",revenue_display:display("133.1"),sources:[source],yoy_growth_percent:"10.00",three_year_cagr_percent:"10.00",three_period_cagr_percent:"15.37",growth_comparisons:growth}}],annual_totals:{"2025":{period_end:"2025-12-31",revenue_display:display("120")}},ttm:{period_end:"2026-06-30",revenue_display:display("133.1"),prior_year_revenue_display:display("121")}})!;
 return {id:"SEC",symbol:"SEC",name:"SEC issuer",country:"USA",currency:"US$ millions",sector:"Manufacturing",description:"",reportingPeriod:"FY2025",updatedAt:"today",metrics:{revenue:[{year:2025,value:120}],operatingMargin:[],freeCashFlow:[],netDebt:[]},notes:{growth:"",profitability:"",cash:"",debt:""},companyStory:{revenueOfferings:story}};
}
describe("SEC revenue growth contract",()=>{
 it("displays the supplied true-three-year field and retains comparison provenance",()=>{
  const c=company();const item=c.companyStory!.revenueOfferings!.offerings[0];
  expect(item.ttmThreeYearCagrPercent).toBe(10);
  expect(item.ttmGrowthComparisons).toMatchObject({cagr_years:3,three_year_prior_ttm:{sources:[source]}});
  const html=renderToStaticMarkup(<HowItMakesMoney company={c}/>);
  expect(html.match(/10\.0%/g)).toHaveLength(2);
  expect(html).not.toContain("15.4%");
  expect(html).toContain("three years earlier using a cube root");
 });
 it("does not substitute a legacy two-year calculation for a missing true-three-year result",()=>{
  const c=company(),item=c.companyStory!.revenueOfferings!.offerings[0];
  item.ttmThreeYearCagrPercent=null;
  item.ttmThreeYearCagrReason="A standard CAGR cannot be calculated for negative revenue.";
  const html=renderToStaticMarkup(<HowItMakesMoney company={c}/>);
  expect(html).not.toContain("15.4%");
  expect(html).toContain('title="A standard CAGR cannot be calculated for negative revenue."');
 });
});
