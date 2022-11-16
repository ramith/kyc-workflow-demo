import ballerina/http;
import ramith/maps_api;
import ballerina/log;
//import ramith/kyc_api;

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

    resource function post validate(@http:Payload Customer[] customers) returns error? {

        maps_api:Client maps_apiEp = check new (clientConfig = {
            auth: {
                clientId: clientId,
                clientSecret: clientSecret
            }
        });

        foreach Customer c in customers {
            log:printInfo("validating customer:", accountId = c.accountId, name = string `${c.firstName} ${c.lastName}`);
            maps_api:Address| error postMapsAddressValidateResponse =  maps_apiEp->postMapsAddressValidate(payload = c.address);
            
        }
    }
}
