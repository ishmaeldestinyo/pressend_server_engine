pub async fn get_supported_cryptocurrencies() -> impl Responder {
    HttpResponse::Ok().json()
} 

pub async fn execute_order() -> impl Responder {
    // check if order lock limit is still valid
    // execute the payment
}

pub async fn get_locked_order() -> impl Responder {

}

pub async fn get_exchange_rate() -> impl Responder {
    // get exchange rate
}