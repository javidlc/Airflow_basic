/*QUESTION 4*/   
   
 /*1. What are the main differences from a population point of view (i.e. higher population of under 30s)/
  *  between the best performing “neighbourhood_cleansed”and the worst/ 
 /(in terms of estimated revenue per active listings) over the last 12 months?*/    

/*LGA*/

(select m1.lga_code,m2.lga_name, round(m1.revenue,0) as est_revenue, m1.count_listings,round(m1.revenue/m1.count_listings,0) as revn_list,m2.med_age, m2.totpob, m2.tot_p_un55,m2.tot_p_up55,
		m2.average_household_size as av_hs_siz, m2.australian_citizen_p as aus_ctz, m2.indigenous_p_tot_p as ind_pop
from(
	select
 	star.apartments.lga_code,
 	SUM((30 - (star.apartments.availability_30))*(star.apartments.price)) as revenue,
 	count(distinct(id)) as count_listings
 	from star.apartments 
 	left join star.census
 	on UPPER(star.apartments.lga_code) = UPPER(star.census.lga_code_2016)
 	where has_availability = 't'
 	group by lga_code
 	ORDER by revenue DESC
 	LIMIT 1) m1
 	left join (
 	select star.census.median_age_persons as med_age, star.census.tot_p_p as totpob, star.census.lga_code_2016 as lga_code_2016, raw."location".lga_name as lga_name,
 	star.census.tot_p_un55, star.census.tot_p_up55,star.census.average_household_size, star.census.australian_citizen_p , star.census.indigenous_p_tot_p 
 	from star.census
 	left join raw."location"
 	on star.census.lga_code_2016= raw."location".lga_code) m2
 	on m1.lga_code = m2.lga_code_2016
 	limit 1)
union 
	(
	select m1.lga_code,m2.lga_name,round(m1.revenue,0) as est_revenue, m1.count_listings, round(m1.revenue/m1.count_listings,0) as revn_list,m2.med_age, m2.totpob, m2.tot_p_un55,m2.tot_p_up55,
		m2.average_household_size as av_hs_siz, m2.australian_citizen_p as aus_ctz, m2.indigenous_p_tot_p as ind_pop
from(
	select
 	star.apartments.lga_code,
 	SUM((30 - (star.apartments.availability_30))*(star.apartments.price)) as revenue,
 	count(distinct(id)) as count_listings
 	from star.apartments 
 	left join star.census
 	on UPPER(star.apartments.lga_code) = UPPER(star.census.lga_code_2016)
 	where has_availability = 't'
 	group by lga_code
 	ORDER by revenue ASC
 	LIMIT 1) m1
 	left join (
 	select star.census.median_age_persons as med_age, star.census.tot_p_p as totpob, star.census.lga_code_2016 as lga_code_2016, raw."location".lga_name as lga_name,
 	star.census.tot_p_un55, star.census.tot_p_up55,star.census.average_household_size, star.census.australian_citizen_p , star.census.indigenous_p_tot_p 
 	from star.census
 	left join raw."location"
 	on star.census.lga_code_2016= raw."location".lga_code) m2
 	on m1.lga_code = m2.lga_code_2016
 	limit 1)
 	
 	
/*2. What will be the best type of listing (property type, room type and accommodates for) /
   * for the top 5 “neighbourhood_cleansed” (in terms of estimated revenue per active listing) /
   * to have the highest number of stays?*/ 
  
/*LGA with more revenues*/ 

select distinct(lga_code), lga_name, round(revenue,0) as revenue
from
(select
 	star.apartments.lga_code,
 	SUM((30 - (star.apartments.availability_30))*(star.apartments.price)) as revenue
 	from star.apartments 
 	where has_availability = 't'
 	group by star.apartments.lga_code
 	ORDER by revenue desc
 	LIMIT 5) m1
 	left join 
 	(select lga_code, lga_name
 	from raw.location) m2
 	using(lga_code)
 	order by revenue desc
 	
/*lga_code for all the combinations of lga, property, room type and accommodates*/ 	

select distinct(lga_code), lga_name, round(revenue,0) as revenue,property_type,room_type,accommodates
from(
 	select
 	star.apartments.lga_code,
 	star.apartments.property_type,
 	star.apartments.room_type,
 	star.apartments.accommodates,
 	SUM((30 - (star.apartments.availability_30))*(star.apartments.price)) as revenue 
 	from star.apartments 
 	where has_availability = 't' 
 	group by star.apartments.lga_code,property_type, room_type,accommodates
 	ORDER by revenue desc) m1
 	left join
 	(select lga_code, lga_name
 	from raw.location) m2
 	using(lga_code)
 	order by revenue desc
 		

/*see the lga with the highest revenue and its most expensive listings */
 
 	(select
 	star.apartments.lga_code,
 	star.apartments.property_type,
 	star.apartments.room_type,
 	star.apartments.accommodates,
 	count(distinct(id)) as count_listings,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price)),0) as revenue,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price))/count(distinct(id)),0) as rev_list
 	from star.apartments
 	where has_availability = 't' AND star.apartments.lga_code IN ('LGA15990')
 	group by star.apartments.lga_code,property_type, room_type,accommodates
 	ORDER by revenue desc
 	limit 1)
 union
  (select
 	star.apartments.lga_code,
 	star.apartments.property_type,
 	star.apartments.room_type,
 	star.apartments.accommodates,
 	count(distinct(id)) as count_listings,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price)),0) as revenue,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price))/count(distinct(id)),0) as rev_list
 	from star.apartments
 	where has_availability = 't' AND star.apartments.lga_code IN ('LGA17200')
 	group by star.apartments.lga_code,property_type, room_type,accommodates
 	ORDER by revenue desc
 	limit 1)
 union 
  (select
 	star.apartments.lga_code,
 	star.apartments.property_type,
 	star.apartments.room_type,
 	star.apartments.accommodates,
 	count(distinct(id)) as count_listings,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price)),0) as revenue,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price))/count(distinct(id)),0) as rev_list
 	from star.apartments
 	where has_availability = 't' AND star.apartments.lga_code IN ('LGA18050')
 	group by star.apartments.lga_code,property_type, room_type,accommodates
 	ORDER by revenue desc
 	limit 1)
 union
  (select
 	star.apartments.lga_code,
 	star.apartments.property_type,
 	star.apartments.room_type,
 	star.apartments.accommodates,
 	count(distinct(id)) as count_listings,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price)),0) as revenue,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price))/count(distinct(id)),0) as rev_list
 	from star.apartments
 	where has_availability = 't' AND star.apartments.lga_code IN ('LGA16550')
 	group by star.apartments.lga_code,property_type, room_type,accommodates
 	ORDER by revenue desc
 	limit 1)
 union
  (select
 	star.apartments.lga_code,
 	star.apartments.property_type,
 	star.apartments.room_type,
 	star.apartments.accommodates,
 	count(distinct(id)) as count_listings,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price)),0) as revenue,
 	round(SUM((30 - (star.apartments.availability_30))*(star.apartments.price))/count(distinct(id)),0) as rev_list
 	from star.apartments
 	where has_availability = 't' AND star.apartments.lga_code IN ('LGA14170')
 	group by star.apartments.lga_code,property_type, room_type,accommodates
 	ORDER by revenue desc
 	limit 1)
 	order by revenue desc
 	

/*3.Do hosts with multiple listings are more inclined to /
 * have their listings in the same “neighbourhood” as where they live?*/
 
/* hosts with more than 1 listing, 4,957*/ 	
select count(*)
from star.owners
where host_tot_list_new > 1 

/*looking hosts with more than one listings*/
 
 select host_id,tot_qt_equalLGA, tot_qt, proportion
 from(
 select host_id, host_tot_list_new, lga_code
 from star.owners
 where host_tot_list_new > 1) m1 
 left join (
 select host_id, count(lga_host),count(lga_listing),count(id) as tot_qt_equalLGA ,round(avg(host_tot_list_new),0) as tot_qt, round(count(id)/avg(host_tot_list_new),1) as proportion
 from(
 select m2.host_id, m2.lga_code as lga_host, m1.lga_code as lga_listing, m1.id, host_tot_list_new  
 from star.apartments AS m1
 left join
 star.owners AS m2
 on m1.host_id=m2.host_id 
 where host_tot_list_new>1
 group by (m2.host_id,m2.lga_code,m1.lga_code, m1.id)) as m2
 where(m2.lga_host = m2.lga_listing)
 group by host_id) as m3
 using (host_id)
 
 
 /*looking hosts with more than one listing and live outside their listings*/
 
 select count(*)
 from(
 select host_id, host_tot_list_new, lga_code
 from star.owners
 where host_tot_list_new > 1) m1 
 left join (
 select host_id, count(lga_host),count(lga_listing),count(id) as tot_qt_equalLGA ,round(avg(host_tot_list_new),0) as tot_qt, round(count(id)/avg(host_tot_list_new),1) as proportion
 from(
 select m2.host_id, m2.lga_code as lga_host, m1.lga_code as lga_listing, m1.id, host_tot_list_new  
 from star.apartments AS m1
 left join
 star.owners AS m2
 on m1.host_id=m2.host_id 
 where host_tot_list_new>1
 group by (m2.host_id,m2.lga_code,m1.lga_code, m1.id)) as m2
 where(m2.lga_host = m2.lga_listing)
 group by host_id) as m3
 using (host_id)
 where proportion is null
 
 /*looking hosts with more than one listing and live outside NSW*/
 
 select count(*)
 from(
 select host_id, host_tot_list_new, lga_code
 from star.owners
 where host_tot_list_new > 1) m1 
 left join (
 select host_id, count(lga_host),count(lga_listing),count(id) as tot_qt_equalLGA ,round(avg(host_tot_list_new),0) as tot_qt, round(count(id)/avg(host_tot_list_new),1) as proportion
 from(
 select m2.host_id, m2.lga_code as lga_host, m1.lga_code as lga_listing, m1.id, host_tot_list_new  
 from star.apartments AS m1
 left join
 star.owners AS m2
 on m1.host_id=m2.host_id 
 where host_tot_list_new>1
 group by (m2.host_id,m2.lga_code,m1.lga_code, m1.id)) as m2
 where(m2.lga_host = m2.lga_listing)
 group by host_id) as m3
 using (host_id)
 where lga_code = 'Out of NSW'
 
 /*looking hosts with more than one listings and match at least in one*/
 select count(*)
 from(
 select host_id, host_tot_list_new
 from star.owners
 where host_tot_list_new > 1) m1 
 left join (
 select host_id, count(lga_host),count(lga_listing),count(id) as tot_qt_equalLGA ,round(avg(host_tot_list_new),0) as tot_qt, round(count(id)/avg(host_tot_list_new),1) as proportion
 from(
 select m2.host_id, m2.lga_code as lga_host, m1.lga_code as lga_listing, m1.id, host_tot_list_new  
 from star.apartments AS m1
 left join
 star.owners AS m2
 on m1.host_id=m2.host_id 
 where host_tot_list_new>1
 group by (m2.host_id,m2.lga_code,m1.lga_code, m1.id)) as m2
 where(m2.lga_host = m2.lga_listing)
 group by host_id) as m3
 using (host_id)
 where proportion is not null
 
  /*looking hosts with more than one listing and match at least in 0,5 of their listings --> change to =1 to see all that match*/
  select count(*)
 from(
 select host_id, host_tot_list_new
 from star.owners
 where host_tot_list_new > 1) m1 
 left join (
 select host_id, count(lga_host),count(lga_listing),count(id) as tot_qt_equalLGA ,round(avg(host_tot_list_new),0) as tot_qt, round(count(id)/avg(host_tot_list_new),1) as proportion
 from(
 select m2.host_id, m2.lga_code as lga_host, m1.lga_code as lga_listing, m1.id, host_tot_list_new  
 from star.apartments AS m1
 left join
 star.owners AS m2
 on m1.host_id=m2.host_id 
 where host_tot_list_new>1
 group by (m2.host_id,m2.lga_code,m1.lga_code, m1.id)) as m2
 where(m2.lga_host = m2.lga_listing)
 group by host_id) as m3
 using (host_id)
 where proportion >0.5
 
 
 /*4.For hosts with a unique listing, does their estimated revenue over the last 12 months/
   can cover the annualised median mortgage repayment of their listing’s “neighbourhood_cleansed”?*/

/*total host unique listing, 25,360 */
 
 select count(*)
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as unique_listing, sum((30- availability_30)*price) as revenue
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,unique_listing, xx.lga_code,xxx.median_mortgage_repay_monthly
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1 
 
 select *
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as count1, sum((30- availability_30)*price) as revenue
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,count1, xx.lga_code,xxx.median_mortgage_repay_monthly
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1 
 
 /*total host unique listing and revenue more than repayment, 22,566 */
 
select count(*)
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as count1, sum((30- availability_30)*price) as revenue
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,count1, xx.lga_code,xxx.median_mortgage_repay_monthly
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1
 where m1.revenue>m1.median_mortgage_repay_monthly*12
 
 select *, median_mortgage_repay_monthly*12 as anualised_median_mortgage_repay_monthly
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as count1, sum((30- availability_30)*price) as revenue, count(distinct(month)) as num_month
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,count1,num_month, xx.lga_code,xxx.median_mortgage_repay_monthly 
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1
 where m1.revenue>m1.median_mortgage_repay_monthly*12 
 
 /*total host unique listing and revenue more than repayment and the listings was for less than 12 months */
 
 select *, median_mortgage_repay_monthly*12 as anualised_median_mortgage_repay_monthly
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as count1, sum((30- availability_30)*price) as revenue, count(distinct(month)) as num_month
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,count1,num_month, xx.lga_code,xxx.median_mortgage_repay_monthly 
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1
 where m1.revenue>m1.median_mortgage_repay_monthly*12 and num_month < 12
 
 /*total host unique listing and revenue more than repayment and profits */
 
 select *, median_mortgage_repay_monthly*12 as anualised_median_mortgage_repay_monthly, revenue-median_mortgage_repay_monthly*12 as profit
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as count1, sum((30- availability_30)*price) as revenue, count(distinct(month)) as num_month
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,count1,num_month, xx.lga_code,xxx.median_mortgage_repay_monthly 
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1
 where m1.revenue>m1.median_mortgage_repay_monthly*12 
 
 /*total host unique listing and revenue less or equal than repayment */
 
 select count(*)
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as count1, sum((30- availability_30)*price) as revenue
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,count1, xx.lga_code,xxx.median_mortgage_repay_monthly
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1
 where m1.revenue<m1.median_mortgage_repay_monthly*12
 
 select count(*)
from(
 with x as (
 select host_id as host_id, count(distinct(id)) as count1, sum((30- availability_30)*price) as revenue
 from star.apartments
 group by host_id
 having(count(distinct(id))= 1))
 SELECT distinct(x.host_id), revenue,count1, xx.lga_code,xxx.median_mortgage_repay_monthly
        from x
 left join (select lga_code, host_id from star.apartments) xx
 on x.host_id = xx.host_id
 left join (select median_mortgage_repay_monthly,lga_code_2016 from star.census) xxx
 on xx.lga_code = xxx.lga_code_2016) as m1
 where m1.revenue=m1.median_mortgage_repay_monthly*12
 
 
 

        
  
 

 	
 	
 	