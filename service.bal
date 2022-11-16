import ballerina/http;
import ramith/maps_api;
import ballerina/log;
import ramith/kyc_api;

configurable string clientSecret = ?;

configurable string clientId = ?;

type Customer record {
    string accountId;
    string firstName;
    string lastName;
    maps_api:Address address;
};

# A service representing a network-accessible API
# bound to port `9090`.
service / on new http:Listener(9090) {

    resource function post validate(@http:Payload Customer[] customers) returns string|error {

        maps_api:Client maps_apiEp = check new (clientConfig = {
            auth: {
                clientId: clientId,
                clientSecret: clientSecret
            }
        });

        kyc_api:Client kyc_apiEp = check new (clientConfig = {
            auth: {
                clientId: clientId,
                clientSecret: clientSecret
            }
        });

        foreach Customer c in customers {
            log:printInfo("validating customer:", accountId = c.accountId, name = string `${c.firstName} ${c.lastName}`);
            maps_api:Address|error postMapsAddressValidateResponse = maps_apiEp->postMapsAddressValidate(payload = c.address);
            if (postMapsAddressValidateResponse is error) {
                log:printError(string `Address is invalid for customer: ${c.firstName}`);
            } else {
                log:printInfo(string `Address is valid for customer: ${c.firstName}`);
                kyc_api:KycInfo|error postMapsKycAccountidResponse = check kyc_apiEp->postMapsKycAccountid(accountId = "123");
                if postMapsKycAccountidResponse is error {
                    log:printError(string `KYC is invalid for customer: ${c.firstName}`);
                } else {
                    log:printInfo(string `KYC is valid for customer: ${c.firstName}. Adding to the database...`);
                }
            }
        }
        return "ok";
    }
}
