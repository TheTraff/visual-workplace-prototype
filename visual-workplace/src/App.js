import React, { Component }  from 'react';
import { Subscription } from 'react-apollo';
import gql from 'graphql-tag';
import logo from './logo.svg';
import './App.css';

const fulfillments_subscription = gql`
  subscription {
    current_fulfillments {
      f_id
      state
      state_start_time
    }
  }
`

function hashCode(str) { // java String#hashCode
    var hash = 0;
    for (var i = 0; i < str.length; i++) {
       hash = str.charCodeAt(i) + ((hash << 5) - hash);
    }
    return hash;
} 

function intToRGB(i){
    var c = (i & 0x00FFFFFF)
        .toString(16)
        .toUpperCase();

    return "#" + "00000".substring(0, 6 - c.length) + c;
}

export class FulfillmentBox extends Component {
  render() {
    let color = intToRGB(hashCode(this.props.item.f_id.toString()));
    let startTime = new Date(this.props.item.state_start_time + "Z");
    const element = 
      <div style={{color: color}}>
        <em style={{color: color}}>{this.props.item.f_id + " | " + startTime.toLocaleString()}</em>
      </div>
    return element
  }
}

export class StateList extends Component {
  render() {

    const element = <ul>
      {this.props.item.map((item, index) => (
        <li key={item.f_id}>
          <FulfillmentBox item={item} key={item.f_id}/>
        </li>
      ))}
    </ul>

    return element
  }
}

export class StateTracker extends Component {
  render() {
    const element = <div style={{display: 'flex'}}>
      {Object.keys(this.props.item).map((item, index) => (
        <StateList item={this.props.item[item]}/>
      ))}
    </div>
    return element
  }
}

class App extends Component {
  render() {
    return (
      <div
        style={{display: 'flex', alignItems: 'center', justifyContent: 'center', margin: '20px'}}
      >
        <Subscription subscription={fulfillments_subscription}>
          {
            ({data, error, loading}) => {
              if (error) {
                console.error(error);
                return "Error";
              }
              if (loading) {
                return "Loading";
              }
              let chartJSData = {
                labels: [],
                datasets: [{
                  label: "Fulfillments",
                  data: [],
                  pointBackgroundColor: [],
                  borderColor: 'brown',
                  fill: false
                }]
              };

              let aggregateValues = {
                ENTERING: 0,
                WAITING: 0,
                LAUNCHING: 0,
                TESTING: 0,
                DONE: 0,
                UNKNOWN: 0
              }
              let states = {
                ENTERING: [],
                WAITING: [],
                LAUNCHING: [],
                TESTING: [],
                DONE: [],
                UNKNOWN: []
              }

              let fulfillments = []
              data.current_fulfillments.forEach((item) => {
                states[item.state].push(item);
                const fulfillmentId = item.f_id;
                fulfillments.push([item.f_id, item.state])
                aggregateValues[item.state] += 1;
                chartJSData.labels.push(fulfillmentId);
                chartJSData.datasets[0].data.push(item.state);
              })

              return (
                <div>
                  <div>
                    {JSON.stringify(aggregateValues, null, 2)}
                  </div>
                  <div>
                    <StateTracker item={states}/>
                  </div>
                </div>
              );
            }
          }
        </Subscription>
      </div>
    );
  }
}

/*
function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
      </header>
    </div>
  );
}*/

export default App;
